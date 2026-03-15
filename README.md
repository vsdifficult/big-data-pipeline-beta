# Big Data Pipeline Beta

This repository implements a **beta-scale production-ready distributed data pipeline** for continuously moving user comments from PostgreSQL into an ML inference system.

It supports:

- Batch + streaming ingestion
- Kafka-based decoupled transport
- Spark Structured Streaming distributed processing
- GPU-ready ML worker service for sentiment, toxicity, and embeddings
- Scalable output storage (ClickHouse + Data Lake Parquet)
- Kubernetes deployment, autoscaling, and observability

## Quick Navigation

- Full architecture and scaling strategy: [`docs/architecture.md`](docs/architecture.md)
- PostgreSQL extraction: [`src/extraction/cdc_poller.py`](src/extraction/cdc_poller.py)
- Kafka producer: [`src/streaming/kafka_producer.py`](src/streaming/kafka_producer.py)
- Kafka consumer example: [`src/streaming/kafka_consumer.py`](src/streaming/kafka_consumer.py)
- Spark Structured Streaming job: [`src/processing/spark_comment_pipeline.py`](src/processing/spark_comment_pipeline.py)
- ML inference worker service: [`src/ml/ml_worker.py`](src/ml/ml_worker.py)
- Kubernetes deployment manifests: [`deployment/k8s`](deployment/k8s)
- Dockerfiles: [`deployment/docker`](deployment/docker)

## Pipeline Topics

- `comments.raw`: extracted comments from DB CDC/incremental extraction
- `comments.cleaned`: validated and normalized comments
- `comments.ml_tasks`: batched tasks routed to ML workers
- `comments.ml_results`: inference results output

## Message Schema (JSON/Avro-friendly)

```json
{
  "comment_id": 123,
  "user_id": 999,
  "text": "Great product!",
  "language": "en",
  "created_at": "2026-03-15T09:30:00Z",
  "metadata": {"source": "mobile"}
}
```

## Local Validation

```bash
python -m py_compile src/common/schemas.py src/extraction/cdc_poller.py src/streaming/kafka_producer.py src/streaming/kafka_consumer.py src/processing/spark_comment_pipeline.py src/ml/ml_worker.py
```

## Notes

This codebase is designed as a production-oriented template. It includes idempotency strategies, checkpointing, retries, and observability hooks required for reliable operation at very large scale.
