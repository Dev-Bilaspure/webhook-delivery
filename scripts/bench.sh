#!/usr/bin/env bash
# Runs a scenario against a locally running stack, one JSON result per rep into bench/.
set -euo pipefail

cd "$(dirname "$0")/.."

RECEIVERS=(8080 8081 8082)
STATS="http://localhost:${RECEIVERS[0]}"
API="http://localhost:8000"
KAFKA=webhook-kafka
OUT=bench

REPS=${REPS:-3}
PROFILE=${PROFILE:-steady}
RATE=${RATE:-300}
CONCURRENCY=${CONCURRENCY:-100}
EVENTS=${EVENTS:-3000}
KEYS=${KEYS:-400}
SETTLE_TIMEOUT=${SETTLE_TIMEOUT:-300}
BREAKER_COOLDOWN=${BREAKER_COOLDOWN:-45}

faults_were_set=0

fail() { echo "error: $*" >&2; exit 1; }

preflight() {
  curl -sf "$API/healthz" >/dev/null || fail "api not reachable at $API, start it first"
  for p in "${RECEIVERS[@]}"; do
    curl -sf "http://localhost:$p/stats" >/dev/null || fail "receiver not reachable on :$p"
  done
  docker exec "$KAFKA" true >/dev/null 2>&1 || fail "kafka container '$KAFKA' not running"
  case "$PROFILE" in steady|burst|all) ;; *) fail "PROFILE must be steady, burst or all" ;; esac
}

fault() { curl -sf -XPOST "http://localhost:$1/control" -d "$2" >/dev/null || fail "control :$1 failed"; }

clear_faults() { for p in "${RECEIVERS[@]}"; do fault "$p" '{"mode":"ok"}'; done; }

lag() {
  docker exec "$KAFKA" /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group "$1" 2>/dev/null |
    awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}'
}

# A previous run can leave events parked in retries on a 45s breaker cooldown. Waiting for
# arrivals to pause is not enough; the gap between cooldowns looks identical to being finished.
# Both consumer groups reaching zero lag is the real signal that nothing is still in flight.
settle() {
  local waited=0 quiet=0

  # A failing event makes the retry topic oscillate (consumed, republished, consumed), so one
  # zero reading can be the dip between cycles rather than a real drain.
  while [ "$waited" -lt "$SETTLE_TIMEOUT" ]; do
    if [ "$(lag delivery-worker)" = "0" ] && [ "$(lag retry-worker)" = "0" ]; then
      quiet=$((quiet + 1))
      [ "$quiet" -ge 3 ] && return 0
    else
      quiet=0
    fi
    sleep 5
    waited=$((waited + 5))
  done

  echo "  still draining after ${SETTLE_TIMEOUT}s (events=$(lag delivery-worker) retries=$(lag retry-worker))" >&2
  return 1
}

reset_store() { curl -sf -XPOST "$STATS/reset" >/dev/null || fail "reset failed"; }

depth() {
  docker exec "$KAFKA" /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic "$1" 2>/dev/null |
    awk -F: '{s+=$3} END {print s+0}'
}

load_flags() {
  if [ "$PROFILE" = "steady" ]; then
    echo "-profile steady -rate $RATE"
  else
    echo "-profile burst -concurrency $CONCURRENCY"
  fi
}

# Each scenario sets its faults and returns how long to wait for delivery to finish. A run that
# opens a breaker needs far longer than the backoff ladder alone, since the delivery worker and
# the retry worker each hold their own cooldown.
arrange() {
  case "$1" in
    baseline)  echo "20s" ;;
    permanent) fault 8081 '{"mode":"status","status":400}'; echo "30s" ;;
    bulkhead)  fault 8081 '{"mode":"hang","delay":"15s"}'; echo "60s" ;;
    breaker)   fault 8081 '{"mode":"status","status":500}'; echo "150s" ;;
    chaos)     for p in "${RECEIVERS[@]}"; do
                 fault "$p" '{"mode":"flap","status":500,"failures":3,"successes":10}'
               done
               echo "150s" ;;
    *)         fail "unknown scenario '$1'" ;;
  esac
}

# The breaker only closes again if the host is healed while the run is still going.
schedule() {
  case "$1" in
    breaker) (sleep 30; fault 8081 '{"mode":"ok"}'; echo "  [t+30s] healed :8081" >&2) & ;;
  esac
}

run_once() {
  local scenario=$1 rep=$2 quiet dlq_before dlq_after retries_before retries_after out

  clear_faults

  if [ "$faults_were_set" = "1" ]; then
    echo "  waiting ${BREAKER_COOLDOWN}s for breakers opened by the previous run to close"
    sleep "$BREAKER_COOLDOWN"
  fi

  if ! settle; then
    echo "  skipping: the system has not drained, results would include an earlier run" >&2
    return 0
  fi
  reset_store
  dlq_before=$(depth dead-letter)
  retries_before=$(depth retries)

  quiet=$(arrange "$scenario")
  schedule "$scenario"

  [ "$scenario" = "baseline" ] && faults_were_set=0 || faults_were_set=1

  out="$OUT/$scenario-$PROFILE-$rep.json"
  # shellcheck disable=SC2046,SC2086
  go run ./cmd/tester -json $(load_flags) \
    -events "$EVENTS" -keys "$KEYS" -drain-quiet "$quiet" -drain-timeout 10m \
    2>/dev/null > "$out.tmp"

  dlq_after=$(depth dead-letter)
  retries_after=$(depth retries)

  python3 - "$out.tmp" "$scenario" "$rep" \
    "$((dlq_after - dlq_before))" "$((retries_after - retries_before))" > "$out" <<'PY'
import json, sys
run = json.load(open(sys.argv[1]))
run["scenario"] = sys.argv[2]
run["rep"] = int(sys.argv[3])
run["deadLetterDelta"] = int(sys.argv[4])
run["retriesDelta"] = int(sys.argv[5])
json.dump(run, sys.stdout, indent=2)
PY
  rm -f "$out.tmp"

  python3 - "$out" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
s = r["stats"]; fa = s["firstAttempt"]
print("  submitted=%d arrived=%d accepted=%d refused=%d unaccounted=%d dlq=+%d retries=+%d" % (
    r["submitted"], s["deliveries"], s["accepted"], s["rejected"], r["unaccounted"],
    r["deadLetterDelta"], r["retriesDelta"]))
print("  accept=%.0f/s collisions=%d  p50=%.1f p95=%.1f p99=%.1f  inversions=%d" % (
    r["acceptRatePerSec"], r["keyCollisions"], fa["p50Ms"], fa["p95Ms"], fa["p99Ms"], s["inversions"]))
PY
}

main() {
  local scenario=${1:-all} scenarios profiles
  case "$scenario" in
    -h|--help) fail "usage: $0 [scenario|all]   (baseline permanent bulkhead breaker chaos)" ;;
  esac

  preflight
  mkdir -p "$OUT"

  if [ "$scenario" = "all" ]; then
    scenarios="baseline permanent bulkhead breaker chaos"
  else
    scenarios="$scenario"
  fi

  if [ "$PROFILE" = "all" ]; then
    profiles="steady burst"
  else
    profiles="$PROFILE"
  fi

  echo "profiles=$profiles scenarios=$scenarios events=$EVENTS keys=$KEYS reps=$REPS"

  for PROFILE in $profiles; do
    for s in $scenarios; do
      for rep in $(seq 1 "$REPS"); do
        echo "== $s ($PROFILE) rep $rep/$REPS"
        run_once "$s" "$rep"
      done
    done
  done

  clear_faults
  echo
  python3 scripts/summarise.py "$OUT"
}

main "$@"
