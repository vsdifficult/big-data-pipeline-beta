from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    expr,
    from_json,
    length,
    lower,
    regexp_replace,
    to_json,
    struct,
    trim,
    when,
)
from pyspark.sql.types import LongType, StringType, StructField, StructType


INPUT_SCHEMA = StructType(
    [
        StructField("comment_id", LongType(), nullable=False),
        StructField("user_id", LongType(), nullable=False),
        StructField("text", StringType(), nullable=False),
        StructField("language", StringType(), nullable=True),
        StructField("created_at", StringType(), nullable=False),
    ]
)


def build_session() -> SparkSession:
    return (
        SparkSession.builder.appName("comment-ml-pipeline")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "400"))
        .getOrCreate()
    )


def run() -> None:
    spark = build_session()
    kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", os.environ.get("RAW_TOPIC", "comments.raw"))
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", os.environ.get("MAX_OFFSETS_PER_TRIGGER", "500000"))
        .load()
    )

    parsed = raw.select(from_json(col("value").cast("string"), INPUT_SCHEMA).alias("c")).select("c.*")

    cleaned = (
        parsed.filter(col("text").isNotNull() & (length(trim(col("text"))) > 0))
        .withColumn("text", regexp_replace(lower(trim(col("text"))), r"\\s+", " "))
        .withColumn("language", when(col("language").isNull(), expr("'und'"))
                    .otherwise(lower(col("language"))))
    )

    routed = cleaned.withColumn(
        "task_type",
        when(length(col("text")) < 32, expr("'light'"))
        .when(length(col("text")) < 280, expr("'standard'"))
        .otherwise(expr("'heavy'")),
    )

    cleaned_out = cleaned.selectExpr("CAST(comment_id AS STRING) AS key", "to_json(struct(*)) AS value")
    tasks_out = routed.selectExpr("CAST(comment_id AS STRING) AS key", "to_json(struct(*)) AS value")

    checkpoint_base = os.environ.get("CHECKPOINT_BASE", "s3a://comment-pipeline-checkpoints")

    q1 = (
        cleaned_out.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", os.environ.get("CLEANED_TOPIC", "comments.cleaned"))
        .option("checkpointLocation", f"{checkpoint_base}/cleaned")
        .outputMode("append")
        .start()
    )

    q2 = (
        tasks_out.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", os.environ.get("ML_TASKS_TOPIC", "comments.ml_tasks"))
        .option("checkpointLocation", f"{checkpoint_base}/ml_tasks")
        .outputMode("append")
        .start()
    )

    q1.awaitTermination()
    q2.awaitTermination()


if __name__ == "__main__":
    run()
