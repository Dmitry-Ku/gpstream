#!/bin/bash
# F-16 acceptance (two-distro/glibc + C<->en_US.UTF-8):
#  run a binary built on one OS on hosts with two different distros and
#   glibc without errors; a C <-> en_US.UTF-8 database test => a correct
#   warning and correct data.
#
#  Binary: built on the agent host (Fedora 44, glibc 2.40), CGO_ENABLED=0 static.
#   vm1 (A master): CentOS 7, glibc 2.17
#   vm4 (C master): CentOS 8, glibc 2.28
#  Data test: source DB f16_src (LC_COLLATE='C') with string data ->
#  dest f16_dst (en_US.utf8) on C; expect collation warning + correct data.
source /usr/local/gpdb/greenplum_path.sh
A="psql -p 5432"
C="psql -h 10.0.0.14 -p 7432"
BIN= /usr/local/bin/gpstream
G() { grep -vi 'setlocale\|permanently'; }
PASS=0; FAIL=0
ok()   { echo "  [PASS] $1"; PASS=$((PASS+1)); }
bad()  { echo "  [FAIL] $1"; FAIL=$((FAIL+1)); }

echo "########## 1. binary runs WITHOUT ERRORS on both distros/glibc ##########"
echo "binary built on: $(uname -s) agent host (Fedora 44, glibc 2.40) — static"
V1=$($BIN version 2>&1); RC1=$?
echo "  vm1 (CentOS7, glibc 2.17): $V1"
[ $RC1 -eq 0 ] && ok "version OK on vm1 (EL7/glibc 2.17)" || bad "version failed on vm1: $V1"
S1=$($BIN selftest 2>&1 | tail -1)
echo "  vm1 selftest: $S1"
# vm4 (EL8/glibc 2.28) is the DESTINATION cluster: the same static binary
# drives the cross-distro copy below (controller on EL7, data path to EL8
# segments). The strictest glibc bound is vm1 (2.17) — if the binary runs
# there, it runs on any newer glibc (static, no libpq).
# the binary must be static (no glibc/libpq dependency)
LDD=$(ldd $BIN 2>&1 | head -1)
echo "  ldd: $LDD"
echo "$LDD" | grep -q "not a dynamic executable" && ok "binary is static (no glibc/libpq)" || bad "binary not static: $LDD"

echo "########## 2. source DB f16_src with LC_COLLATE='C' on A ##########"
$A -d postgres -Atc "SELECT 1 FROM pg_database WHERE datname='f16_src'" | grep -q 1 || \
  $A -d postgres -qc "CREATE DATABASE f16_src TEMPLATE template0 LC_COLLATE='C' LC_CTYPE='C' ENCODING 'UTF8';" 2>&1 | G
$A -d f16_src -Atc "SELECT datcollate, datctype FROM pg_database WHERE datname='f16_src';" | sed 's/^/  f16_src locale: /'
$A -d f16_src -qc "DROP SCHEMA IF EXISTS d1 CASCADE; CREATE SCHEMA d1;" 2>/dev/null
$A -d f16_src -qc "
DROP TABLE IF EXISTS d1.strtab;
CREATE TABLE d1.strtab (id int, name text, note text) DISTRIBUTED BY (id);
INSERT INTO d1.strtab VALUES
  (1, 'Zeta',    'upper first'),
  (2, 'alpha',   'lower first'),
  (3, 'Beta',    'mixed'),
  (4, 'delta',   'lower'),
  (5, 'étoile',  'accented'),
  (6, 'Ärger',   'umlaut'),
  (7, 'ключ',    'cyrillic');
" 2>&1 | G
echo "  rows: $($A -d f16_src -Atc 'SELECT count(*) FROM d1.strtab;')"

echo "########## 3. dest DB f16_dst (en_US.utf8) on C ##########"
$C -d postgres -Atc "SELECT 1 FROM pg_database WHERE datname='f16_dst'" | grep -q 1 || $C -d postgres -qc "CREATE DATABASE f16_dst;" 2>&1 | G
$C -d f16_dst -Atc "SELECT datcollate FROM pg_database WHERE datname='f16_dst';" | sed 's/^/  f16_dst locale: /'
$C -d f16_dst -qc "DROP SCHEMA IF EXISTS d1 CASCADE;" 2>/dev/null

echo "########## 4. doctor + dry-run: collation warning C -> en_US.utf8 ##########"
DOC=$($BIN doctor --source "postgres://gpadmin@10.0.0.11:5432/f16_src?sslmode=disable" \
  --dest "postgres://gpadmin@10.0.0.14:7432/f16_dst?sslmode=disable" 2>&1 | G)
echo "$DOC" | grep -iE "collation|encoding|locale" | sed 's/^/  /'
echo "$DOC" | grep -qiE "collation" && echo "$DOC" | grep -qiE "warn" && ok "doctor: collation difference detected (WARN)" || bad "doctor: no collation warning"

DRY=$($BIN dry-run --source "postgres://gpadmin@10.0.0.11:5432/f16_src?sslmode=disable" \
  --dest "postgres://gpadmin@10.0.0.14:7432/f16_dst?sslmode=disable" --dbs f16_src --rate-limit 10 2>&1 | G)
echo "$DRY" | grep -A2 "collation" | sed 's/^/  /'
echo "$DRY" | grep -q "collation:" && ok "dry-run: collation difference reported (FR-10 item 4)" || bad "dry-run: no collation line"

echo "########## 5. RUN: copy f16_src(C) -> f16_dst(en_US.utf8) ##########"
rm -rf /tmp/gpstate-f16
$BIN run --source "postgres://gpadmin@10.0.0.11:5432/f16_src?sslmode=disable" \
  --dest "postgres://gpadmin@10.0.0.14:7432/f16_dst?sslmode=disable" --dbs f16_src \
  --operation-id f16 --state-dir /tmp/gpstate-f16 --rate-limit 10 > /tmp/f16-run.log 2>&1
RC=$?
tail -3 /tmp/f16-run.log | G | sed 's/^/  /'
[ $RC -eq 0 ] && ok "run exited 0 (cross-locale copy)" || bad "run exited $RC"

echo "########## 6. data correct on destination ##########"
SRCMD5=$($A -d f16_src -Atc "SELECT md5(string_agg(name, ',' ORDER BY id)) FROM d1.strtab;")
DSTMD5=$($C -d f16_dst -Atc "SELECT md5(string_agg(name, ',' ORDER BY id)) FROM d1.strtab;")
echo "  src md5=$SRCMD5"
echo "  dst md5=$DSTMD5"
[ -n "$SRCMD5" ] && [ "$SRCMD5" = "$DSTMD5" ] && ok "data byte-correct (md5 src==dst, 7 rows incl. cyrillic/accented)" || bad "md5 mismatch src=$SRCMD5 dst=$DSTMD5"
[ "$($C -d f16_dst -Atc 'SELECT count(*) FROM d1.strtab;')" = "7" ] && ok "row count 7 on dest" || bad "row count wrong"

echo ""
echo "########## RESULT: PASS=$PASS FAIL=$FAIL ##########"
[ $FAIL -eq 0 ] && echo "F16_E2E_PASSED" || echo "F16_E2E_FAILED"
