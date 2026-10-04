#!/bin/bash
# F09 fault-injection (live GP6): RST on the DESTINATION data conns mid-copy.
#
# Scope safety: gpstream runs as uid `gp` (1000); GPDB runs as `gpadmin` (1001).
# The iptables rule REJECTs (tcp-reset) only locally-generated packets from
# uid 1000 to :6432 (the destination coordinator) — so ONLY gpstream's dest
# connections get RST'd. The clusters (uid 1001), the source conns (:5432),
# and everything else are untouched.
#
# Usage (on vm1, as the `gp` ssh user, with passwordless sudo):
#   bash f09-fault-injection.sh [INJECT_AT=12] [HOLD_FOR=8]
# Phases: 1) run + RST mid-copy -> some units fail ("connection reset by peer");
#         2) `gpstream resume` re-queues the non-terminal units;
#         3) re-run retries failed units, keeps swapped ones (checksum-skip).
# Verify: per-table counts + order-independent md5 digests A == B.
# Proven live on the lab 28.09 (12M rows, 8 tables): 3 units failed
# under the fault, all 8 recovered, 8/8 counts + digests match, no leftover
# iptables rules, clusters healthy after.
export LC_ALL=C
PSQL=/usr/local/gpdb/bin/psql
SRC="host=10.0.0.11 port=5432 user=gpadmin dbname=f09 sslmode=disable"
DST="host=10.0.0.11 port=6432 user=gpadmin dbname=f09 sslmode=disable"
PG="host=10.0.0.11 port=5432 user=gpadmin dbname=postgres sslmode=disable"
PG6="host=10.0.0.11 port=6432 user=gpadmin dbname=postgres sslmode=disable"
gppsql() { sudo -u gpadmin $PSQL "$@" 2>&1 | grep -vE "setlocale|lang.sh"; }

INJECT_AT=${1:-12}   # seconds after run start
HOLD_FOR=${2:-8}     # seconds the RST stays active
OP=f09-1
STATE=/tmp/gs-f09

# Safety net: ALWAYS remove the iptables rule on exit (even if interrupted),
# so a leftover RST rule can't break later operations. Scoped to uid 1000 (gp)
# -> :6432, so it only ever touches gpstream's destination conns.
RULE="-p tcp --dport 6432 -m owner --uid-owner 1000 -j REJECT --reject-with tcp-reset"
cleanup_ipt() {
  sudo iptables -D OUTPUT $RULE 2>/dev/null
}
trap cleanup_ipt EXIT

echo "=== reset f09 DBs (A + B) ==="
gppsql "$PG"  -c "DROP DATABASE IF EXISTS f09" >/dev/null
gppsql "$PG"  -c "CREATE DATABASE f09" >/dev/null
gppsql "$PG6" -c "DROP DATABASE IF EXISTS f09" >/dev/null
gppsql "$PG6" -c "CREATE DATABASE f09" >/dev/null

echo "=== create 8 x 1.5M on A (f09) ==="
for i in 01 02 03 04 05 06 07 08; do
  gppsql "$SRC" -q -c \
    "CREATE TABLE f09_t$i (id int, val numeric(18,4), txt text, ts timestamp) DISTRIBUTED BY (id);
     INSERT INTO f09_t$i SELECT g, (g*1.2345)::numeric(18,4), 'row_'||g, now() FROM generate_series(1,1500000) g;" >/dev/null
done
AROWS=$(gppsql "$SRC" -Atc "SELECT sum(c) FROM (SELECT count(*) c FROM f09_t01 UNION ALL SELECT count(*) c FROM f09_t02 UNION ALL SELECT count(*) c FROM f09_t03 UNION ALL SELECT count(*) c FROM f09_t04 UNION ALL SELECT count(*) c FROM f09_t05 UNION ALL SELECT count(*) c FROM f09_t06 UNION ALL SELECT count(*) c FROM f09_t07 UNION ALL SELECT count(*) c FROM f09_t08) s")
echo "A rows: $AROWS"

rm -rf $STATE /tmp/f09-run.log

echo "=== PHASE 1: run + RST injection at +${INJECT_AT}s (hold ${HOLD_FOR}s) ==="
(
  /usr/bin/gpstream run \
    --source "$SRC" --dest "$DST" \
    --dbs f09 --overwrite \
    --source-agent-binary /usr/bin/gpstream-source-agent \
    --dest-agent-binary /usr/bin/gpstream-dest-agent \
    --operation-id $OP --state-dir $STATE \
    --streams 4 --verify full
) > /tmp/f09-run.log 2>&1 &
RUNPID=$!
sleep $INJECT_AT
echo "[inject] iptables RST uid=1000 -> :6432 (tcp-reset), hold ${HOLD_FOR}s"
sudo iptables -I OUTPUT $RULE
sleep $HOLD_FOR
echo "[inject] removing iptables rule"
sudo iptables -D OUTPUT $RULE
wait $RUNPID
RUN1_EXIT=$?
echo "PHASE1_EXIT=$RUN1_EXIT"
grep -E "run:|unit.*fail|conflict|error" /tmp/f09-run.log | tail -8
echo "--- journal: LATEST state per unit after phase 1 ---"
awk -F'"unit_id":' '{split($2,a,","); uid=a[1]} /"state":/{match($0,/"state":"[a-z]+"/); s=substr($0,RSTART+9,RLENGTH-10); last[uid]=s} END{for(u in last) print "unit "u": "last[u]}' $STATE/$OP/journal.jsonl | sort -t' ' -k2 -n
echo "PHASE1_LOG=/tmp/f09-run.log"

echo "=== PHASE 2: resume (re-queue non-terminal) ==="
/usr/bin/gpstream resume --operation-id $OP --state-dir $STATE 2>&1 | grep -vE "setlocale"
echo "=== PHASE 3: re-run (retries failed units, keeps completed) ==="
T0=$(date +%s)
/usr/bin/gpstream run \
  --source "$SRC" --dest "$DST" \
  --dbs f09 --overwrite \
    --operation-id $OP --state-dir $STATE \
  --streams 4 --verify full 2>&1 | grep -vE "setlocale" | grep -E "units:|run:|conflicts:"
T1=$(date +%s)
echo "PHASE3_SECONDS=$((T1-T0))"

echo "=== VERIFY: counts A vs B ==="
for i in 01 02 03 04 05 06 07 08; do
  A=$(gppsql "$SRC" -Atc "SELECT count(*) FROM f09_t$i")
  B=$(sudo -u gpadmin $PSQL "$DST" -Atc "SELECT count(*) FROM f09_t$i" 2>&1 | grep -vE "setlocale|lang.sh")
  if [ "$A" = "$B" ]; then echo "f09_t$i OK $A"; else echo "f09_t$i COUNT MISMATCH A=$A B=$B"; fi
done
echo "=== VERIFY: digests A vs B ==="
DM=0
for i in 01 02 03 04 05 06 07 08; do
  D1=$(gppsql "$SRC" -Atc "SELECT md5(string_agg(id::text||','||val||','||txt||','||ts, ',' ORDER BY id)) FROM f09_t$i")
  D2=$(sudo -u gpadmin $PSQL "$DST" -Atc "SELECT md5(string_agg(id::text||','||val||','||txt||','||ts, ',' ORDER BY id)) FROM f09_t$i" 2>&1 | grep -vE "setlocale|lang.sh")
  if [ "$D1" = "$D2" ]; then echo "f09_t$i digest OK"; else echo "f09_t$i DIGEST MISMATCH"; DM=1; fi
done
[ $DM -eq 0 ] && echo "ALL DIGESTS MATCH" || echo "DIGEST FAILURES"
echo "=== F09 FAULT TEST DONE ==="
