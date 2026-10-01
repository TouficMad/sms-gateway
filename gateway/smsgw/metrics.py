"""Prometheus metrics shared by the API and the workers."""
from prometheus_client import Counter, Gauge, Histogram

API_MESSAGES = Counter("sms_api_messages_total", "Messages received by the API", ["result"])

SUBMITS = Counter("sms_submit_total", "submit_sm attempts", ["carrier", "route", "result"])
SUBMIT_LATENCY = Histogram(
    "sms_submit_latency_seconds", "Time from submit_sm to submit_sm_resp", ["carrier"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
FAILOVERS = Counter("sms_failover_total", "Messages moved to the next carrier", ["from_carrier"])
SEGMENTS = Counter("sms_segments_total", "SMS segments sent (billing units)", ["carrier", "route"])
MESSAGES_FINAL = Counter("sms_messages_final_total", "Messages by final status",
                         ["route", "status"])

DLRS = Counter("sms_dlr_total", "Delivery receipts", ["carrier", "route", "stat"])
DLR_LATENCY = Histogram(
    "sms_dlr_latency_seconds", "Time from submit to delivery receipt", ["carrier"],
    buckets=(0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)
END_TO_END = Histogram(
    "sms_end_to_end_seconds", "Time from API accept to delivery receipt", ["route"],
    buckets=(0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)

CARRIER_CONNECTED = Gauge("sms_carrier_connected", "1 if the SMPP session is bound", ["carrier"])
CARRIER_CIRCUIT_OPEN = Gauge("sms_carrier_circuit_open", "1 if the circuit breaker is open",
                             ["carrier"])
QUEUE_BACKLOG = Gauge("sms_queue_backlog", "Messages queued or in flight")
