#!/usr/bin/env bash
# End-to-end check against LocalStack, with the real Kinesis connector:
#   1. fresh LocalStack with the streams and table,
#   2. 40 minutes of synthetic plant logs with two known incidents, written as fast as possible
#      (a catch-up backlog: the case that breaks per-reader watermarks),
#   3. the detector job, run locally until its output settles,
#   4. checks: both incidents alerted, few false alerts, no late events, every window scored.
#
# Requirements: docker, and either mvn + JDK 17 on the PATH or MVN pointing at an equivalent wrapper.
# Usage: local/e2e.sh            (from the repository root)
set -euo pipefail
cd "$(dirname "$0")/.."

MVN=${MVN:-mvn}
WORK=$(mktemp -d)
COMPOSE="docker compose -f local/docker-compose.yml"
JOB_PID=""

cleanup() {
  if [[ -n "$JOB_PID" ]]; then kill "$JOB_PID" 2>/dev/null || true; wait "$JOB_PID" 2>/dev/null || true; fi
  ${E2E_CLEANUP:-true}
}
trap cleanup EXIT

aws() { docker exec local-localstack-1 awslocal "$@"; }

# Prints every record of a stream, one JSON document per line
read_stream() {
  local stream=$1 shard it
  for shard in $(aws kinesis list-shards --stream-name "$stream" --query 'Shards[].ShardId' --output text); do
    it=$(aws kinesis get-shard-iterator --stream-name "$stream" --shard-id "$shard" \
      --shard-iterator-type TRIM_HORIZON --query ShardIterator --output text)
    aws kinesis get-records --shard-iterator "$it" --limit 10000 --query 'Records[].Data' --output text \
      | tr '\t' '\n' | while read -r data; do [[ -n "$data" ]] && echo "$data" | base64 -d && echo; done
  done
}

echo "==> Fresh LocalStack"
$COMPOSE down -v >/dev/null 2>&1 || true
$COMPOSE up -d --wait >/dev/null

echo "==> Generating 40 minutes of logs"
docker build -q -t zorblatt-loggen log-generator >/dev/null
START=$(date -u -d '-45 min' +%Y-%m-%dT%H:%M:00Z)
docker run --rm --network host -u "$(id -u)" -v "$WORK:/out" \
  -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test zorblatt-loggen \
  --sink kinesis --stream plant-logs --region eu-west-1 --endpoint-url http://localhost:4566 \
  --lines-per-record 5 --seed 21 --start "$START" --duration 2400 --speed 0 --anomaly-rate 0 \
  --inject "KILN-01:spike:1800:180:zone temp" --inject "PRESS-02:error_burst:2100:60" \
  --labels-file /out/labels.jsonl

echo "==> Running the detector (log: $WORK/job.log)"
$MVN -q compile exec:exec >"$WORK/job.log" 2>&1 &
JOB_PID=$!

# Wait until the scores table stops growing
previous=-1
for _ in $(seq 1 60); do
  sleep 10
  count=$(aws dynamodb scan --table-name plant-scores --select COUNT --query Count --output text)
  echo "    scored windows: $count"
  if [[ "$count" -gt 0 && "$count" -eq "$previous" ]]; then break; fi
  previous=$count
done

read_stream plant-anomalies >"$WORK/alerts.jsonl"
read_stream plant-dead-letters >"$WORK/dead-letters.jsonl"

echo "==> Checking"
python3 - "$WORK" "$count" <<'PY'
import collections, json, sys

work, scored = sys.argv[1], int(sys.argv[2])
load = lambda name: [json.loads(line) for line in open(f"{work}/{name}") if line.strip()]
labels, alerts, dead = load("labels.jsonl"), load("alerts.jsonl"), load("dead-letters.jsonl")

failures = []
for label in labels:
    hits = [a for a in alerts if a["stationId"] == label["station"]
            and label["start"] * 1000 <= a["windowEnd"] <= label["end"] * 1000 + 120_000]
    status = f"alert {hits[0]['summary']!r}" if hits else "NOT DETECTED"
    print(f"    {label['station']:<12} {label['kind']:<12} {status}")
    if not hits:
        failures.append(f"{label['station']} {label['kind']} not detected")

matched = lambda a: any(a["stationId"] == l["station"] and l["start"] * 1000 <= a["windowEnd"] <= l["end"] * 1000 + 120_000
                        for l in labels)
false_alerts = [a for a in alerts if not matched(a)]
print(f"    alerts: {len(alerts)}, false alerts: {len(false_alerts)}")
for a in false_alerts:
    print(f"      false: {a['summary']}")
if len(false_alerts) > 4:
    failures.append(f"{len(false_alerts)} false alerts")

reasons = collections.Counter(d["reason"] for d in dead)
print(f"    dead letters: {dict(reasons)}")
if reasons.get("LATE_EVENT", 0):
    failures.append(f"{reasons['LATE_EVENT']} late events: watermarks are not tracked per shard")

# 7 stations x 240 windows of 10 s, minus the last open windows
print(f"    scored windows: {scored}")
if scored < 7 * 230:
    failures.append(f"only {scored} windows scored")

if failures:
    print("FAILED: " + "; ".join(failures))
    sys.exit(1)
print("PASSED")
PY
