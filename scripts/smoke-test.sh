#!/usr/bin/env bash
# End-to-end check against a running stack (docker compose up -d --wait).
# Sends messages to every route, waits for delivery receipts, then takes
# carrier-a down and checks that traffic fails over to carrier-b.
set -euo pipefail

API=${API:-http://localhost:8080}
CARRIER_A=${CARRIER_A:-http://localhost:8101}

send() {  # send <to> <text> -> prints message id
  curl -fsS -X POST "$API/api/v1/messages" -H 'Content-Type: application/json' \
    -d "{\"to\":\"$1\",\"from\":\"TOUFIC\",\"text\":\"$2\"}" |
    python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])'
}

wait_final() {  # wait_final <id> -> prints "STATUS CARRIER"
  for _ in $(seq 1 60); do
    out=$(curl -fsS "$API/api/v1/messages/$1" |
      python3 -c 'import sys, json; m = json.load(sys.stdin); print(m["status"], m.get("carrier", "-"))')
    case "$out" in DELIVERED*|UNDELIVERED*|FAILED*) echo "$out"; return 0 ;; esac
    sleep 1
  done
  echo "TIMEOUT -"; return 1
}

fail=0
echo "== Sending one message per route"
for to in +447700900123 +96170123456 +9611234567 +33612345678 +971501234567 +12025550123; do
  id=$(send "$to" "Smoke test code 1234")
  read -r status carrier < <(wait_final "$id")
  printf '%-15s %-12s via %s\n' "$to" "$status" "$carrier"
  [[ "$status" == "FAILED" || "$status" == "TIMEOUT" ]] && fail=1
done

echo "== Arabic (UCS-2) message"
id=$(send "+96170123456" "مرحبا")
read -r status carrier < <(wait_final "$id")
echo "UCS2 message: $status via $carrier"
[[ "$status" == "FAILED" || "$status" == "TIMEOUT" ]] && fail=1

echo "== Failover: taking carrier-a down"
curl -fsS -X PUT "$CARRIER_A/faults" -H 'Content-Type: application/json' -d '{"down": true}' > /dev/null
sleep 2
id=$(send "+447700900123" "Failover test")
read -r status carrier < <(wait_final "$id")
echo "UK message with carrier-a down: $status via $carrier"
[[ "$carrier" == "carrier-b" && "$status" != "FAILED" ]] || fail=1
curl -fsS -X DELETE "$CARRIER_A/faults" > /dev/null

if [[ $fail -ne 0 ]]; then
  echo "SMOKE TEST FAILED"; exit 1
fi
echo "SMOKE TEST PASSED"
