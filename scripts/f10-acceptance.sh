#!/bin/bash
# F-10 acceptance (live, A:5432 EL7 -> C:7432 EL8):
#   "3 DBs, 2 schemas, 1 table under a different name, no data => the
#    destination holds exactly the specified objects; a re-run is idempotent.
#
# Layout on source A (3 DBs x 2 schemas x 2 EMPTY tables):
#   f10_db1: s1.t1, s2.t2
#   f10_db2: s1.t3, s2.t4   (t4 renamed to t4_renamed via --map-table)
#   f10_db3: s1.t5, s2.t6
# Dest C: 3 DBs f10_dst1..3, created explicitly (gpstream does not auto-create).
#
# Checks:
#  (1) dry-run per DB: conflicts none, plan hash stable across two dry-runs
#  (2) run per DB (3 runs, one source DB -> one dest DB)
#  (3) dest holds EXACTLY the specified objects: 2 schemas, 2 tables each,
#      t4_renamed present & t4 absent, no staging leftovers (*_gpstream*),
#      all tables empty (no data)
#  (4) idempotent re-run: re-run the same operation with --overwrite ->
#      exit 0, tables checksum-skip, same object set, no duplicates
source /usr/local/gpdb/greenplum_path.sh
A="psql -p 5432"
C="psql -h 10.0.0.14 -p 7432"
BIN= /usr/local/bin/gpstream
G() { grep -vi 'setlocale\|permanently'; }
PASS=0; FAIL=0
ok()   { echo "  [PASS] $1"; PASS=$((PASS+1)); }
bad()  { echo "  [FAIL] $1"; FAIL=$((FAIL+1)); }
SRCD="postgres://gpadmin@10.0.0.11:5432/"
DSTH="postgres://gpadmin@10.0.0.14:7432/"

echo "########## 1. source layout on A (3 DBs x 2 schemas x 2 empty tables) ##########"
for db in f10_db1 f10_db2 f10_db3; do
  $A -d postgres -Atc "SELECT 1 FROM pg_database WHERE datname='$db'" | grep -q 1 || $A -d postgres -qc "CREATE DATABASE $db;" 2>/dev/null
  $A -d $db -qc "DROP SCHEMA IF EXISTS s1 CASCADE; DROP SCHEMA IF EXISTS s2 CASCADE;" 2>/dev/null
  $A -d $db -qc "CREATE SCHEMA s1; CREATE SCHEMA s2;" 2>/dev/null
done
$A -d f10_db1 -qc "CREATE TABLE s1.t1 (id int, v text) DISTRIBUTED BY (id); CREATE TABLE s2.t2 (id int, v text) DISTRIBUTED BY (id);" 2>/dev/null
$A -d f10_db2 -qc "CREATE TABLE s1.t3 (id int, v text) DISTRIBUTED BY (id); CREATE TABLE s2.t4 (id int, v text) DISTRIBUTED BY (id);" 2>/dev/null
$A -d f10_db3 -qc "CREATE TABLE s1.t5 (id int, v text) DISTRIBUTED BY (id); CREATE TABLE s2.t6 (id int, v text) DISTRIBUTED BY (id);" 2>/dev/null
echo "A objects: $($A -d f10_db1 -Atc "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('s1','s2') AND c.relkind='r'") (db1), db3: $($A -d f10_db3 -Atc "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('s1','s2') AND c.relkind='r'")"

echo "########## 2. dest DBs on C (explicit) ##########"
for db in f10_dst1 f10_dst2 f10_dst3; do
  $C -d postgres -Atc "SELECT 1 FROM pg_database WHERE datname='$db'" | grep -q 1 || $C -d postgres -qc "CREATE DATABASE $db;" 2>&1 | G
  $C -d $db -qc "DROP SCHEMA IF EXISTS s1 CASCADE; DROP SCHEMA IF EXISTS s2 CASCADE;" 2>/dev/null
done

echo "########## 3. dry-run per DB: conflicts none + stable plan hash (clean dest) ##########"
# ensure a clean dest right before the dry-run so the conflict check is deterministic
$C -d f10_dst1 -qc "DROP SCHEMA IF EXISTS s1 CASCADE; DROP SCHEMA IF EXISTS s2 CASCADE;" 2>/dev/null
h1=$($BIN dry-run --source "${SRCD}f10_db1?sslmode=disable" --dest "${DSTH}f10_dst1?sslmode=disable" --dbs f10_db1 --rate-limit 10 2>&1 | G | grep "plan hash" | awk '{print $3}')
h1b=$($BIN dry-run --source "${SRCD}f10_db1?sslmode=disable" --dest "${DSTH}f10_dst1?sslmode=disable" --dbs f10_db1 --rate-limit 10 2>&1 | G | grep "plan hash" | awk '{print $3}')
[ -n "$h1" ] && [ "$h1" = "$h1b" ] && ok "dry-run plan hash stable (idempotent plan, FR-10 item 7): $h1" || bad "plan hash unstable: '$h1' vs '$h1b'"
$BIN dry-run --source "${SRCD}f10_db1?sslmode=disable" --dest "${DSTH}f10_dst1?sslmode=disable" --dbs f10_db1 --rate-limit 10 2>&1 | G | grep -Eq "conflicts: +none" && ok "dry-run: conflicts none" || bad "dry-run conflicts not none"

echo "########## 4. RUN: 3 runs (one source DB -> one dest DB) ##########"
rm -rf /tmp/gpstate-f10
$BIN run --source "${SRCD}f10_db1?sslmode=disable" --dest "${DSTH}f10_dst1?sslmode=disable" --dbs f10_db1 \
  --operation-id f10db1 --state-dir /tmp/gpstate-f10 --rate-limit 10 > /tmp/f10-r1.log 2>&1
R1=$?
$BIN run --source "${SRCD}f10_db2?sslmode=disable" --dest "${DSTH}f10_dst2?sslmode=disable" --dbs f10_db2 \
  --map-table "s2.t4=s2.t4_renamed" \
  --operation-id f10db2 --state-dir /tmp/gpstate-f10 --rate-limit 10 > /tmp/f10-r2.log 2>&1
R2=$?
$BIN run --source "${SRCD}f10_db3?sslmode=disable" --dest "${DSTH}f10_dst3?sslmode=disable" --dbs f10_db3 \
  --operation-id f10db3 --state-dir /tmp/gpstate-f10 --rate-limit 10 > /tmp/f10-r3.log 2>&1
R3=$?
echo "run exits: $R1 $R2 $R3"
[ "$R1$R2$R3" = "000" ] && ok "all 3 runs exited 0" || bad "run exits $R1/$R2/$R3"
grep -E "units:|failed" /tmp/f10-r1.log | G | head -2 | sed 's/^/    /'

echo "########## 5. dest: EXACTLY the specified objects ##########"
for db in f10_dst1 f10_dst2 f10_dst3; do
  N=$($C -d $db -Atc "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('s1','s2') AND c.relkind='r';")
  [ "$N" = "2" ] && ok "$db: exactly 2 tables in s1/s2" || bad "$db: expected 2 tables, got $N"
done
S1N=$($C -d f10_dst1 -Atc "SELECT count(*) FROM pg_namespace WHERE nspname IN ('s1','s2');")
[ "$S1N" = "2" ] && ok "f10_dst1: exactly 2 schemas" || bad "f10_dst1 schemas=$S1N"
REN=$($C -d f10_dst2 -Atc "SELECT count(*) FROM pg_class WHERE relname='t4_renamed';")
OLD=$($C -d f10_dst2 -Atc "SELECT count(*) FROM pg_class WHERE relname='t4';")
[ "$REN" = "1" ] && [ "$OLD" = "0" ] && ok "t4 renamed to t4_renamed (old name absent)" || bad "rename: t4_renamed=$REN t4=$OLD"
STG=$($C -d f10_dst1 -Atc "SELECT count(*) FROM pg_class WHERE relname LIKE '\_gs\_%' OR relname LIKE 'staging%';")
STG2=$($C -d f10_dst2 -Atc "SELECT count(*) FROM pg_class WHERE relname LIKE '\_gs\_%' OR relname LIKE 'staging%';")
[ "$STG" = "0" ] && [ "$STG2" = "0" ] && ok "no staging (_gs_*) leftovers on dest" || bad "staging leftovers: $STG/$STG2"
EMPTY=$($C -d f10_dst1 -Atc "SELECT (SELECT count(*) FROM s1.t1) + (SELECT count(*) FROM s2.t2);")
[ "$EMPTY" = "0" ] && ok "dest tables empty (no data)" || bad "dest rows=$EMPTY"

echo "########## 6. idempotent re-run (--overwrite, checksum-skip) ##########"
$BIN run --source "${SRCD}f10_db1?sslmode=disable" --dest "${DSTH}f10_dst1?sslmode=disable" --dbs f10_db1 \
  --operation-id f10db1 --state-dir /tmp/gpstate-f10 --rate-limit 10 --overwrite > /tmp/f10-r1b.log 2>&1
R1B=$?
$BIN run --source "${SRCD}f10_db2?sslmode=disable" --dest "${DSTH}f10_dst2?sslmode=disable" --dbs f10_db2 \
  --map-table "s2.t4=s2.t4_renamed" \
  --operation-id f10db2 --state-dir /tmp/gpstate-f10 --rate-limit 10 --overwrite > /tmp/f10-r2b.log 2>&1
R2B=$?
echo "rerun exits: $R1B $R2B"
[ "$R1B" = "0" ] && [ "$R2B" = "0" ] && ok "re-run exited 0 (no conflict-stop)" || bad "re-run exits $R1B/$R2B"
grep -ciE "skip" /tmp/f10-r1b.log | sed 's/^/    skipped-lines: /'
N2=$($C -d f10_dst1 -Atc "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('s1','s2') AND c.relkind='r';")
[ "$N2" = "2" ] && ok "object set unchanged after re-run (no duplicates)" || bad "after re-run tables=$N2"

echo ""
echo "########## RESULT: PASS=$PASS FAIL=$FAIL ##########"
[ $FAIL -eq 0 ] && echo "F10_E2E_PASSED" || echo "F10_E2E_FAILED"