# Monitoring and Alerting

## Prometheus Metrics to Expose

- `comments_extracted_total`
- `comments_extractor_lag_seconds`
- `kafka_producer_errors_total`
- `spark_processed_records_total`
- `spark_batch_latency_ms`
- `ml_inference_requests_total`
- `ml_inference_batch_size`
- `ml_inference_latency_ms`
- `pipeline_dlq_total`

## Grafana Dashboards

1. Ingestion dashboard: DB extraction rate, Kafka ingress, topic lag.
2. Processing dashboard: Spark micro-batch durations and failed batches.
3. ML dashboard: throughput per model, GPU utilization, inference p95.
4. Reliability dashboard: retries, DLQ counts, error rates.

## Suggested Alerts

- Kafka consumer lag > threshold for 10m.
- Spark batch delay > 2x baseline.
- ML inference error rate > 1% for 5m.
- Extractor no-output heartbeat for 2m.
