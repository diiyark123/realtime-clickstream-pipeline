#!/usr/bin/env bash
# One-screen health report of every stage of the pipeline.
# Usage: make check

cd "$(dirname "$0")/.." || exit 1
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"

B=$'\033[1m'; G=$'\033[32m'; R=$'\033[31m'; C=$'\033[36m'; N=$'\033[0m'
head_() { echo; echo "${B}${C}[$1] $2${N}"; }
ok()    { echo "   ${G}✅ $*${N}"; }
bad()   { echo "   ${R}❌ $*${N}"; }
say()   { echo "   $*"; }
psqlq() { docker exec postgres psql -U pipeline -d clickstream -tAc "$1" 2>/dev/null; }
show()  { docker exec postgres psql -U pipeline -d clickstream -c "$1" 2>/dev/null | sed 's/^/   /'; }
PY=python3; [ -x .venv/bin/python ] && PY=.venv/bin/python

echo "${B}Clickstream pipeline: health check   $(date -u '+%H:%M:%S UTC')${N}"

head_ 1 "Containers"
running=$(docker compose ps --status running --format '{{.Service}}' 2>/dev/null | tr '\n' ' ')
for s in kafka postgres producer spark; do
  case " $running " in *" $s "*) ok "$s running" ;; *) bad "$s not running → make up" ;; esac
done

head_ 2 "Producer (fake shoppers → Kafka)"
last=$(docker compose logs --tail 20 producer 2>/dev/null | grep "sent=" | tail -1 | sed 's/.*INFO //')
if [ -n "$last" ]; then ok "Sending"; say "$last"; else bad "No recent progress line. See: docker compose logs producer"; fi

head_ 3 "Kafka (messages stored per lane)"
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic clickstream_events 2>/dev/null \
  | awk -F: '{printf "   lane %s: %s messages\n", $2, $3; t+=$3} END {printf "   total: %d\n", t}'

head_ 4 "Spark (processing every 10 s)"
lines=$(docker compose logs --since 1m spark 2>/dev/null | grep -E "\[(quarantine|product_metrics)\]" | tail -3 | sed 's/^spark *| *//')
if [ -n "$lines" ]; then ok "Writing batches"; echo "$lines" | sed 's/^/   /'; else bad "No batches in the last minute. See: docker compose logs spark"; fi
say "Live dashboard: http://localhost:4040 → Structured Streaming"

head_ 5 "Job 1: Quarantine (broken messages)"
show "SELECT error_reason, COUNT(*) AS messages FROM quarantine_events GROUP BY 1 ORDER BY 2 DESC;"

head_ 6 "Job 2: Data lake (data/lake/events/)"
if $PY -c "import duckdb" 2>/dev/null && ls data/lake/events/*/*.parquet >/dev/null 2>&1; then
  $PY - <<'EOF'
import duckdb
src = "'data/lake/events/**/*.parquet'"
files, events = duckdb.sql(f"SELECT count(DISTINCT filename), count(*) FROM read_parquet({src}, filename=true)").fetchone()
dups = duckdb.sql(f"SELECT count(*) FROM (SELECT event_id FROM {src} GROUP BY 1 HAVING count(*) > 1)").fetchone()[0]
print(f"   {files} files, {events} clean events saved")
print(f"   \033[32m✅ 0 duplicates\033[0m" if dups == 0 else f"   \033[31m❌ {dups} duplicate event_ids\033[0m")
EOF
else say "No lake files yet, or DuckDB missing (.venv/bin/pip install duckdb)"; fi

head_ 7 "Job 3: Metrics (product_metrics_1m, last 3 finished minutes)"
show "SELECT to_char(window_start,'HH24:MI') AS minute_utc, revenue, purchases FROM v_revenue_per_minute ORDER BY window_start DESC LIMIT 3;"
lag=$(psqlq "SELECT extract(epoch FROM now() - MAX(window_end))::int FROM product_metrics_1m")
if [ -n "$lag" ] && [ "$lag" -lt 300 ]; then ok "Newest metrics are ${lag}s behind real time (2–3 min is normal)"
else bad "Metrics are ${lag:-?}s behind. If just started, wait 3 min; else: docker compose logs spark"; fi
echo
