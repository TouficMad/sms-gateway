// k6 load test: docker compose --profile loadtest run --rm k6
// Tune with RATE (messages/s) and DURATION, e.g. -e RATE=200 -e DURATION=5m
import http from "k6/http";
import { check } from "k6";

const BASE = __ENV.GATEWAY_URL || "http://localhost:8080";
const RATE = Number(__ENV.RATE || 50);
const DURATION = __ENV.DURATION || "2m";

export const options = {
  scenarios: {
    sms: {
      executor: "ramping-arrival-rate",
      startRate: Math.max(1, Math.floor(RATE / 10)),
      timeUnit: "1s",
      preAllocatedVUs: 50,
      maxVUs: 200,
      stages: [
        { target: RATE, duration: "20s" },
        { target: RATE, duration: DURATION },
        { target: 0, duration: "10s" },
      ],
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<500"],
    checks: ["rate>0.99"],
  },
};

const DESTINATIONS = [
  "+4477009", // UK
  "+961701", // Lebanon mobile
  "+961123", // Lebanon fixed
  "+336123", // France
  "+9715012", // UAE
];
const TEXTS = [
  "Your verification code is %CODE%",
  "Votre code est %CODE%",
  "رمز التحقق الخاص بك هو %CODE%",
  "Order shipped! Track it at example.com/t/%CODE% {ref} €0 fee",
];

function pick(list) {
  return list[Math.floor(Math.random() * list.length)];
}

function digits(n) {
  let s = "";
  for (let i = 0; i < n; i++) s += Math.floor(Math.random() * 10);
  return s;
}

export default function () {
  const prefix = pick(DESTINATIONS);
  const body = JSON.stringify({
    to: prefix + digits(12 - prefix.length + 1),
    from: "TOUFIC",
    text: pick(TEXTS).replace("%CODE%", digits(6)),
  });
  const res = http.post(`${BASE}/api/v1/messages`, body, {
    headers: { "Content-Type": "application/json" },
  });
  check(res, { "accepted (202)": (r) => r.status === 202 });
}
