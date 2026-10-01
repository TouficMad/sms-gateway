# SMS Gateway

An SMS gateway built the way telecom operators run A2P messaging. A **REST API** accepts messages and queues them in **Redis Streams**. **Workers** route each message by destination prefix to the cheapest carrier, send it over **SMPP 3.4**, fail over when a carrier misbehaves, and track **delivery receipts (DLRs)** until each message is delivered or failed. Everything is monitored with **Prometheus, Alertmanager and Grafana**, autoscaled on Kubernetes with **KEDA**, and tested in CI, including a k6 load test.

Two simulated carriers (SMSCs) are included, so the whole system runs on a laptop and no real SMS is ever sent.

![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?logo=redis&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?logo=kubernetes&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?logo=grafana&logoColor=white)
![k6](https://img.shields.io/badge/k6-7D64FF?logo=k6&logoColor=white)

## Architecture

```
                 POST /api/v1/messages
 Client app ───────────────────────────► API (FastAPI) ──── validates E.164, picks route,
                                           │                counts segments (GSM-7 / UCS-2)
                                           ▼
                                 Redis Stream "sms:outbound"  (consumer group, at-least-once)
                                           │
                     ┌─────────────────────┴─────────────────────┐
                     ▼                                           ▼
                 Worker 1                                    Worker N   (KEDA scales on queue lag)
   longest-prefix route → cheapest carrier first → circuit breaker → TPS limit
                     │  submit_sm                                ▲ deliver_sm (DLR)
                     ▼                                           │
           ┌──── SMPP 3.4 ────┐                        ┌──── SMPP 3.4 ────┐
           │ carrier-a (SMSC) │                        │ carrier-b (SMSC) │
           └──────────────────┘                        └──────────────────┘

 Message status: ACCEPTED → SUBMITTED → DELIVERED / UNDELIVERED   (or FAILED if no carrier accepts it)
 Prometheus scrapes API + workers → alert rules → Alertmanager;  Grafana dashboards
```

## Features

| Area | What it does |
|---|---|
| **SMPP 3.4** | Own async implementation: `bind_transceiver`, `submit_sm`, `deliver_sm`, `enquire_link`, `unbind`, `generic_nack`, TLVs. Several messages in flight at once (windowing), checked against the independent [smpplib](https://github.com/python-smpplib/python-smpplib) in tests |
| **Routing** | Longest-prefix match (e.g. `96170` Lebanon mobile beats `961` Lebanon), carriers ordered by cost per segment, automatic failover to the next carrier |
| **Resilience** | Circuit breaker per carrier, reconnect with exponential backoff, retries, at-least-once queue with crash recovery (`XAUTOCLAIM`), graceful shutdown |
| **Carrier limits** | Token-bucket throttling to each carrier's agreed TPS. `ESME_RTHROTTLED` responses fail over |
| **Encoding** | GSM 03.38 7-bit (with the € and `{}[]` extension table) or UCS-2 for Arabic and emoji. Segment counting (160/153 and 70/67). Long messages use the `message_payload` TLV |
| **Delivery receipts** | Parses SMPP Appendix B receipts plus the `receipted_message_id` TLV, and maps carrier IDs back to messages. Any worker can process any receipt |
| **Observability** | Throughput, delivery ratio, DLR latency, submit latency, failovers, SMPP error codes, queue backlog, billing segments. 7 alerts with unit tests |
| **Kubernetes** | Kustomize manifests, KEDA `ScaledObject` on Redis Stream lag, HPA for the API, NetworkPolicy, hardened pods |

## Quick start

```bash
git clone https://github.com/TouficMad/sms-gateway.git
cd sms-gateway
docker compose up -d --build --wait
./scripts/smoke-test.sh
```

| Service | URL |
|---|---|
| API docs (Swagger UI) | http://localhost:8080/docs |
| Grafana (admin / admin): **SMS Gateway** dashboard | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Alertmanager | http://localhost:9093 |
| carrier-a / carrier-b control APIs | http://localhost:8101/stats, http://localhost:8102/stats |

### Send a message

```bash
curl -X POST localhost:8080/api/v1/messages -H 'Content-Type: application/json' \
  -d '{"to": "+96170123456", "from": "TOUFIC", "text": "مرحبا! Your code is 4821"}'
# {"id":"5f0c…","status":"ACCEPTED","route":"lebanon-mobile","encoding":"UCS2","segments":1}

curl localhost:8080/api/v1/messages/5f0c…
# {"status":"DELIVERED","carrier":"carrier-b","dlr_stat":"DELIVRD", …}
```

### Break a carrier and watch failover

```bash
# carrier-a goes down: sessions dropped, binds refused
curl -X PUT localhost:8101/faults -H 'Content-Type: application/json' -d '{"down": true}'

# or make it unreliable instead
curl -X PUT localhost:8101/faults -H 'Content-Type: application/json' \
  -d '{"submit_error_rate": 0.5, "delivery_rate": 0.6}'

curl localhost:8080/api/v1/routes      # live carrier state per route
curl -X DELETE localhost:8101/faults   # back to normal
```

In Grafana you'll see failovers rise, traffic move to carrier-b, and the `SmsCarrierDisconnected` alert fire in Alertmanager.

### Load test

```bash
docker compose --profile loadtest run --rm -e RATE=200 -e DURATION=2m k6
```

The test sends to every route with a mix of English, French and Arabic texts. It fails if more than 1% of requests error or p95 latency goes over 500 ms.

## API

| Method | Path | Description |
|---|---|---|
| POST | `/api/v1/messages` | Queue a message: `{"to": "+E164", "from": "SENDER", "text": "..."}` → `202` |
| GET | `/api/v1/messages/{id}` | Status, carrier, segments, DLR status and error |
| GET | `/api/v1/routes` | Routes, prefixes, carrier cost and live health |
| GET | `/health` | Liveness, including Redis |
| GET | `/metrics` | Prometheus metrics |

Set `API_KEYS=key1,key2` to require an `X-API-Key` header.

## Routing configuration

Carriers and routes are defined in [`config/routes.yaml`](config/routes.yaml). Secrets are read from environment variables:

```yaml
carriers:
  carrier-a:
    host: ${CARRIER_A_HOST:-localhost}
    password: ${CARRIER_A_PASSWORD:-secret}
    tps: 100
routes:
  - name: lebanon-mobile
    prefixes: ["9613", "96170", "96171", "96176", "96178", "96179", "96181"]
    carriers:
      - {carrier: carrier-b, cost: 0.0310}   # cheapest: tried first
      - {carrier: carrier-a, cost: 0.0350}   # failover
```

## Deploy to Kubernetes

```bash
# KEDA is needed for queue-based autoscaling of the workers
helm repo add kedacore https://kedacore.github.io/charts
helm install keda kedacore/keda -n keda --create-namespace

kubectl apply -k .
kubectl -n sms port-forward svc/api 8080:80
```

To use real carriers, delete `k8s/carriers.yaml`, set `CARRIER_*_HOST` in `k8s/worker.yaml`, and store the passwords with External Secrets or Sealed Secrets.

## Tests and CI

| Check | Tool |
|---|---|
| Unit tests: SMPP codec, GSM-7/UCS-2, routing, circuit breaker, token bucket | pytest |
| SMPP interoperability with a third-party client | smpplib |
| End-to-end: API → Redis → worker → SMPP → DLR, with failover, circuit breaker, undelivered, all-carriers-down and receipts queued over a reconnect | pytest + real Redis |
| Lint | ruff |
| Alert rules and their unit tests, Alertmanager config | promtool, amtool |
| Kubernetes manifests, including the KEDA CRD | kustomize, kubeconform |
| Full stack smoke test and load test | Docker Compose, k6 |

Every push to `main` publishes the image to `ghcr.io/touficmad/sms-gateway`.

## Project layout

```
gateway/
  smsgw/
    smpp/codec.py     SMPP 3.4 PDUs, TLVs, delivery receipts
    smpp/client.py    async transceiver: windowing, enquire_link, DLR callback
    encoding.py       GSM 03.38 / UCS-2, segment counting
    routing.py        longest-prefix LCR, circuit breaker, token bucket
    store.py          Redis Streams queue + message state
    worker.py         routing, submit with failover, DLR handling
    api.py            FastAPI REST API
    simulator.py      carrier SMSC simulator with fault injection
  tests/
config/routes.yaml    carriers, prefixes, costs, TPS
monitoring/           Prometheus rules + tests, Alertmanager, Grafana dashboard
k8s/ + kustomization.yaml
loadtest/send-sms.js  k6 scenario
scripts/smoke-test.sh
```

## Roadmap

- [ ] Delivery-rate-based routing: deprioritize a carrier whose DLR success drops, like the [VoIP routing monitor](https://github.com/TouficMad/voip-routing-monitor)
- [ ] Concatenated SMS with UDH for carriers without `message_payload` support
- [ ] Per-customer rate limits and billing export
- [ ] Mark messages `UNKNOWN` when no DLR arrives within the validity period
