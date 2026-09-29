package pg

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

// PartitionSpec is a partitioned parent's partitioning clause, captured from
// the source with pg_get_partition_def and reproduced verbatim in the
// destination DDL (Fix 1 / GP6 partitioned tables).
//
// GP6 (PostgreSQL 9.4 base, classic inheritance partitioning) has no
// pg_get_partkeydef, so the partition-key columns are parsed out of the spec
// string ("PARTITION BY RANGE (day) (...)" -> ["day"]) instead of a catalog
// query. The destination CREATE is:
//
//	CREATE TABLE s.t (cols) DISTRIBUTED BY (distkey) <Spec>
//
// where the distribution clause MUST precede the PARTITION BY clause
// (lab, 29.09: "DISTRIBUTED BY" after the partition spec is a syntax
// error on this build).
type PartitionSpec struct {
	// Spec is the verbatim "PARTITION BY ..." clause from
	// pg_get_partition_def (leaf partition names and bounds included). The
	// destination re-creates the same leaf layout; when the destination
	// collation sorts the partition-key values differently, a row lands in a
	// DIFFERENT leaf than on the source — accepted as the correct final
	// layout (the destination cluster owns the data afterwards).
	Spec string
	// KeyColumns are the partition-key column names, in spec order.
	KeyColumns []string
	// HasDefault reports whether the spec contains a DEFAULT partition.
	// A parent WITHOUT a default partition cannot accept a row whose key
	// matches no leaf bound: on a collation difference such a row is
	// rejected and the whole COPY aborts (lab, 29.09: "no partition for
	// partitioning key" mid-COPY). The guard (plan.PartitionGuard) uses this
	// to fail the unit early with a diagnostic instead.
	HasDefault bool
}

var (
	// partKeyRe matches the first "(col[, col...])" after "PARTITION BY"
	// and before the leaf spec's opening "(" — i.e. the partition-key list.
	partKeyRe = regexp.MustCompile(`(?i)PARTITION\s+BY\s+(?:RANGE|LIST|HASH)\s*\(([^)]*)\)`)
	// partDefaultRe matches a DEFAULT partition in the leaf spec.
	partDefaultRe = regexp.MustCompile(`(?i)DEFAULT\s+PARTITION`)
)

// ParsePartitionSpec validates a pg_get_partition_def string and extracts the
// partition-key columns and default-partition flag. An empty input (a plain,
// non-partitioned table) yields a zero PartitionSpec.
func ParsePartitionSpec(spec string) PartitionSpec {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return PartitionSpec{}
	}
	p := PartitionSpec{Spec: spec, HasDefault: partDefaultRe.MatchString(spec)}
	if m := partKeyRe.FindStringSubmatch(spec); m != nil {
		for _, c := range strings.Split(m[1], ",") {
			c = strings.TrimSpace(strings.Trim(c, `"`))
			if c != "" {
				p.KeyColumns = append(p.KeyColumns, c)
			}
		}
	}
	return p
}

// CapturePartitionSpec reads pg_get_partition_def for schema.table and parses
// it. A plain (non-partitioned) table returns a zero PartitionSpec and no
// error — pg_get_partition_def yields NULL for it (lab, 29.09).
func CapturePartitionSpec(q Querier, ctx context.Context, schema, table string) (PartitionSpec, error) {
	rows, err := q.Query(ctx,
		`SELECT pg_get_partition_def(c.oid)
		   FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		  WHERE c.relname = $1 AND n.nspname = $2`, table, schema)
	if err != nil {
		return PartitionSpec{}, fmt.Errorf("query partition spec: %w", err)
	}
	if len(rows) == 0 || rows[0][0] == nil {
		return PartitionSpec{}, nil
	}
	return ParsePartitionSpec(fmt.Sprintf("%v", rows[0][0])), nil
}

// CollationProbeSQL is a fixed statement whose RESULT depends on the
// database's collation: the same set of string literals, sorted, as an
// array. It is deliberately collation-sensitive (quote-prefixed and accented
// literals) so two databases with the SAME locale name but different glibc
// (e.g. CentOS7 vs CentOS8, both "en_US.utf8") yield different orderings.
//
// lab (29.09): A(C7) -> {1,"1",2,a,"a","a",A,e,é,z,Z};
// C(C8) -> {"1",1,2,"a",a,"a",A,e,é,z,Z}. The locale names were identical;
// only the behavioral probe revealed the difference.
const CollationProbeSQL = `SELECT array_agg(x ORDER BY x)
   FROM (VALUES ('a'), ('A'), (chr(34) || 'a'), ('a' || chr(34)),
                ('1'), ('2'), (chr(34) || '1'), (chr(233)), ('e'),
                ('Z'), ('z')) AS v(x)`

// CollationSignature returns the behavioral collation fingerprint of the
// connected database (the CollationProbeSQL result as text). Two databases
// with different glibc collation behavior produce different signatures even
// when their datcollate locale names match. Used by the Fix-1 partition guard
// (plan.PartitionGuard) and the glibc detection (Fix 2): a signature
// difference means string partitioning keys may re-route rows differently on
// the destination.
func CollationSignature(q Querier, ctx context.Context) (string, error) {
	rows, err := q.Query(ctx, CollationProbeSQL)
	if err != nil {
		return "", fmt.Errorf("collation probe: %w", err)
	}
	if len(rows) == 0 || rows[0][0] == nil {
		return "", nil
	}
	return fmt.Sprintf("%v", rows[0][0]), nil
}
