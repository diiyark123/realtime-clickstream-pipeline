#!/usr/bin/env bash
# Guided demo: sends one test purchase (plus a duplicate and a broken copy) and
# follows it through Kafka -> Spark -> quarantine / data lake / metrics, checking each stage.
# Usage: make demo

cd "$(dirname "$0")/.." || exit 1
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"

B=$'\033[1m'; G=$'\033[32m'; R=$'\033[31m'; Y=$'\033[33m'; C=$'\033[36m'; N=$'\033[0m'
step()  { echo; echo "${B}${C}━━━ STEP $1: $2 ━━━${N}"; }
say()   { echo "   $*"; }
ok()    { echo "   ${G}✅ $*${N}"; }
bad()   { echo "   ${R}❌ $*${N}"; }
pause() { if [ -t 0 ]; then echo; read -r -p "   ${Y}Press Enter to continue...${N}" _; fi; }
psqlq() { docker exec postgres psql -U pipeline -d clickstream -tAc "$1" 2>/dev/null; }
show()  { docker exec postgres psql -U pipeline -d clickstream -c "$1" 2>/dev/null | sed 's/^/   /'; }

PY=python3; [ -x .venv/bin/python ] && PY=.venv/bin/python
HAVE_DUCKDB=0; $PY -c "import duckdb" 2>/dev/null && HAVE_DUCKDB=1
lake() { $PY -c "import duckdb,sys; print(duckdb.sql(sys.argv[1]))" "$1" 2>/dev/null | sed 's/^/   /'; }
lakeval() { $PY -c "import duckdb,sys; r=duckdb.sql(sys.argv[1]).fetchone(); print('|'.join(str(x) for x in r))" "$1" 2>/dev/null; }

echo "${B}Clickstream pipeline: guided demo${N}"
say "We'll pretend to be one shopper buying 3 books, and send 3 messages:"
say "  ① a normal purchase   → should be saved and counted"
say "  ② an exact copy of ①  → should be thrown away (duplicate)"
say "  ③ a broken purchase   → should go to quarantine"

# ---------------------------------------------------------------------------
step 0 "Is everything running?"
running=$(docker compose ps --status running --format '{{.Service}}' 2>/dev/null | sort | tr '\n' ' ')
for s in kafka postgres producer spark; do
  case " $running " in *" $s "*) ok "$s is running" ;; *) bad "$s is NOT running"; MISSING=1 ;; esac
done
if [ -n "$MISSING" ]; then say "Start the pipeline with:  make up   (then wait ~3 minutes and run make demo again)"; exit 1; fi
say "Spark dashboard: http://localhost:4040  → tab 'Structured Streaming'"
pause

# ---------------------------------------------------------------------------
step 1 "Send the 3 test messages to Kafka"
ID="MYTEST-$(date +%H%M%S)"
TS=$(date -u +%Y-%m-%dT%H:%M:%S.000+00:00)
MINUTE=$(date -u +"%Y-%m-%d %H:%M:00")
NEXT_MINUTE=$(date -u -v+1M +"%Y-%m-%d %H:%M:00" 2>/dev/null || date -u -d "+1 minute" +"%Y-%m-%d %H:%M:00")
GOOD="{\"event_id\": \"$ID\", \"user_id\": \"U99999\", \"session_id\": \"S-$ID\", \"event_type\": \"purchase\", \"product_id\": \"P4001\", \"category\": \"books\", \"price\": 15.0, \"quantity\": 3, \"device\": \"desktop\", \"country\": \"IN\", \"event_ts\": \"$TS\"}"
BROKEN="{\"event_id\": \"$ID-BAD\", \"user_id\": \"U99999\", \"event_type\": \"purchase\", \"product_id\": \"P4001\", \"event_ts\": \"$TS\"}"

WATCH=$(mktemp)
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic clickstream_events --property print.partition=true --property print.offset=true 2>/dev/null \
  | grep --line-buffered "$ID" > "$WATCH" &
sleep 5   # let the watcher connect before sending

printf 'U99999|%s\nU99999|%s\nU99999|%s\n' "$GOOD" "$GOOD" "$BROKEN" \
  | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
      --topic clickstream_events --property parse.key=true --property key.separator="|"
say "Test ID:   ${B}$ID${N}"
say "Sent at:   ${B}$TS${N} (UTC; India time = UTC + 5:30)"
say "Message ①: $GOOD"
pause

# ---------------------------------------------------------------------------
step 2 "Did Kafka receive them?"
for _ in 1 2 3 4 5 6 7 8 9 10; do [ "$(wc -l < "$WATCH")" -ge 3 ] && break; sleep 1; done
pkill -f "kafka-console-consumer.sh.*clickstream_events" 2>/dev/null
say "Reading Kafka topic 'clickstream_events'. Each message gets a lane (partition) and position (offset):"
cut -c1-110 "$WATCH" | sed 's/^/   /'
GOOD_OFF=$(grep "\"$ID\"" "$WATCH" | head -1 | sed -n 's/.*Offset:\([0-9]*\).*/\1/p')
BAD_OFF=$(grep "$ID-BAD" "$WATCH" | sed -n 's/.*Offset:\([0-9]*\).*/\1/p')
if [ "$(wc -l < "$WATCH")" -ge 3 ]; then ok "All 3 messages are in Kafka (Kafka stores everything, even the copy and the broken one)"
else bad "Expected 3 messages, saw $(wc -l < "$WATCH" | tr -d ' ')"; fi
rm -f "$WATCH"
pause

# ---------------------------------------------------------------------------
step 3 "Spark picks them up (every 10 seconds) and checks each one"
say "Waiting for Spark's next batch..."
for _ in $(seq 1 30); do [ "$(psqlq "SELECT count(*) FROM quarantine_events WHERE raw_value LIKE '%$ID-BAD%'")" = "1" ] && break; sleep 2; done
say "Spark log (latest batches):"
docker compose logs --since 1m spark 2>/dev/null | grep -E "\[(quarantine|product_metrics)\]" | tail -3 | sed 's/^spark *| */   /'
ok "Spark is processing. Live view: http://localhost:4040 → Structured Streaming"
pause

# ---------------------------------------------------------------------------
step 4 "JOB 1, Quarantine: was the broken message ③ caught?"
say "Looking in database table quarantine_events..."
show "SELECT kafka_partition AS lane, kafka_offset AS position, error_reason FROM quarantine_events WHERE raw_value LIKE '%$ID%';"
n=$(psqlq "SELECT count(*) FROM quarantine_events WHERE raw_value LIKE '%$ID%'")
reason=$(psqlq "SELECT error_reason FROM quarantine_events WHERE raw_value LIKE '%$ID%' LIMIT 1")
if [ "$n" = "1" ] && [ "$reason" = "missing:session_id" ]; then
  ok "Exactly 1 broken message caught, reason 'missing:session_id' (we left that field out on purpose)"
  [ -n "$BAD_OFF" ] && ok "Its position ($BAD_OFF) matches Kafka in step 2"
else bad "Expected 1 row with reason missing:session_id, found $n row(s) '$reason'"; fi
pause

# ---------------------------------------------------------------------------
step 5 "JOB 2, Data lake: was ① saved once, and the copy ② removed?"
if [ "$HAVE_DUCKDB" = "1" ]; then
  say "Looking in the archive files under data/lake/events/ ..."
  for _ in $(seq 1 15); do
    [ "$(lakeval "SELECT count(*) FROM 'data/lake/events/**/*.parquet' WHERE event_id LIKE '$ID%'")" -ge 1 ] 2>/dev/null && break; sleep 2
  done
  lake "SELECT event_id, event_type, product_id, price, quantity, kafka_offset AS position FROM 'data/lake/events/**/*.parquet' WHERE event_id LIKE '$ID%'"
  n=$(lakeval "SELECT count(*) FROM 'data/lake/events/**/*.parquet' WHERE event_id LIKE '$ID%'")
  if [ "$n" = "1" ]; then
    ok "Exactly 1 copy saved, although we sent it twice → duplicate removed"
    ok "The broken message is not here (it went to quarantine instead)"
  else bad "Expected 1 row, found $n"; fi
  say "Files are in folder: data/lake/events/event_date=$(date -u +%Y-%m-%d)/"
else
  bad "Skipped: DuckDB not installed. Fix with: python3 -m venv .venv && .venv/bin/pip install duckdb"
fi
pause

# ---------------------------------------------------------------------------
step 6 "JOB 3, Metrics: wait for the minute to close"
say "Spark waits 2 minutes for late messages before writing a minute's totals,"
say "so the numbers never change once written. This takes about 3 minutes."
for i in $(seq 1 40); do
  row=$(psqlq "SELECT views||'|'||add_to_carts||'|'||purchases||'|'||revenue FROM product_metrics_1m WHERE product_id='P4001' AND window_start='$MINUTE+00'")
  [ -n "$row" ] && break
  printf "\r   ⏳ waiting... %3ss" $((i*10)); sleep 10
done
echo
if [ -z "$row" ]; then bad "Metrics row not written yet. Check: docker compose logs spark"; exit 1; fi
say "Database table product_metrics_1m, book P4001, minute starting $MINUTE UTC:"
show "SELECT window_start, product_id, views, add_to_carts, purchases, units_sold, revenue FROM product_metrics_1m WHERE product_id='P4001' AND window_start='$MINUTE+00';"
ok "Your purchase (3 × 15 = 45) is included. Fake shoppers bought the same book in that minute too."
pause

# ---------------------------------------------------------------------------
step 7 "Prove the metrics are correct: recount from the data lake"
if [ "$HAVE_DUCKDB" = "1" ]; then
  lakerow=$(lakeval "SELECT count(*) FILTER (WHERE event_type='page_view'), count(*) FILTER (WHERE event_type='add_to_cart'), count(*) FILTER (WHERE event_type='purchase'), coalesce(sum(price*quantity) FILTER (WHERE event_type='purchase'),0) FROM 'data/lake/events/**/*.parquet' WHERE product_id='P4001' AND event_ts >= '$MINUTE' AND event_ts < '$NEXT_MINUTE'")
  fmt() { echo "$1" | awk -F'|' '{printf "views=%s carts=%s purchases=%s revenue=%.2f", $1, $2, $3, $4}'; }
  say "Metrics table says: $(fmt "$row")"
  say "Data lake recount:  $(fmt "$lakerow")"
  if [ "$(fmt "$row")" = "$(fmt "$lakerow")" ]; then ok "They match exactly → the metrics are correct"
  else bad "They differ (a slow lake write can cause this; run make demo again)"; fi
else say "Skipped (DuckDB not installed)"; fi

# ---------------------------------------------------------------------------
step 8 "Is the pipeline keeping up?"
lag=$(psqlq "SELECT extract(epoch FROM now() - MAX(window_end))::int FROM product_metrics_1m")
say "Newest metrics are $lag seconds behind real time."
if [ "${lag:-9999}" -lt 300 ]; then ok "Healthy (2–3 minutes is normal)"; else bad "Falling behind. Check: docker compose logs spark"; fi

echo
echo "${B}${G}━━━ DONE: you followed one purchase through the whole pipeline ━━━${N}"
say "Kafka got all 3 → Spark checked them → broken one in quarantine → good one saved once in"
say "the data lake → counted in the metrics table → totals verified against the raw data."
