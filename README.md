# Big Data Pipeline Beta

This repository implements a **beta-scale production-ready distributed data pipeline** for continuously moving user comments from PostgreSQL into an ML inference system.

## What is included

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

---

## Как запустить проект локально

Ниже минимальный рабочий сценарий для запуска пайплайна на локальной машине.

### 1) Предварительные требования

- Docker + Docker Compose
- Python 3.11+

### 2) Поднять инфраструктуру (PostgreSQL + Kafka + ClickHouse)

```bash
docker compose -f deployment/local-compose.yml up -d
```

Создать Kafka topics:

```bash
./scripts/create_kafka_topics.sh
```

### 3) Установить Python-зависимости

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 4) Запустить extractor (Postgres -> comments.raw)

```bash
export PYTHONPATH=$(pwd)
export PG_DSN='postgresql://comments:comments@localhost:5432/commentsdb'
export KAFKA_BOOTSTRAP='localhost:9092'
export RAW_TOPIC='comments.raw'
python src/extraction/cdc_poller.py
```

### 5) Запустить Spark processing (comments.raw -> comments.cleaned + comments.ml_tasks)

> Для локального запуска измените checkpoint на локальную папку.

```bash
export PYTHONPATH=$(pwd)
export KAFKA_BOOTSTRAP='localhost:9092'
export RAW_TOPIC='comments.raw'
export CLEANED_TOPIC='comments.cleaned'
export ML_TASKS_TOPIC='comments.ml_tasks'
export CHECKPOINT_BASE='file:///tmp/comment-pipeline-checkpoints'
spark-submit src/processing/spark_comment_pipeline.py
```

### 6) Запустить ML worker (comments.ml_tasks -> comments.ml_results)

```bash
export PYTHONPATH=$(pwd)
export KAFKA_BOOTSTRAP='localhost:9092'
export ML_TASKS_TOPIC='comments.ml_tasks'
export ML_RESULTS_TOPIC='comments.ml_results'
export INFER_BATCH_SIZE='256'
python src/ml/ml_worker.py
```

### 7) Проверить поток сообщений

Consumer по очищенным комментариям:

```bash
export PYTHONPATH=$(pwd)
export TOPIC='comments.cleaned'
python src/streaming/kafka_consumer.py
```

### 8) Сгенерировать тестовую нагрузку (опционально)

```bash
export PYTHONPATH=$(pwd)
export RAW_TOPIC='comments.raw'
export RATE_PER_SEC='5000'
python src/streaming/kafka_producer.py
```

---

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

## Validation

```bash
python -m py_compile src/common/schemas.py src/extraction/cdc_poller.py src/streaming/kafka_producer.py src/streaming/kafka_consumer.py src/processing/spark_comment_pipeline.py src/ml/ml_worker.py
```

## Notes

This codebase is a production-oriented template. It includes idempotency, checkpointing, retry-friendly patterns, and observability hooks required for reliable operation at very large scale.
