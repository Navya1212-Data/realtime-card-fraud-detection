"""
Streaming fraud pipeline: processes landing files one micro-batch at a time.

Each micro-batch: bronze -> quality gate -> de-duplicate
-> features (with card history) -> rules -> silver + gold alerts
-> update card history
"""

from datetime import timedelta

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from delta_utils import append_idempotent, merge_insert_new
from features import TIMELINE_COLUMNS, compute_features
from quality import parse_and_validate
from rules import apply_rules, build_alerts
from spark_session import get_spark

LAKE = "data/stream_lake"
BRONZE = f"{LAKE}/bronze"
QUARANTINE = f"{LAKE}/quarantine"
SILVER = f"{LAKE}/silver"
GOLD = f"{LAKE}/gold_alerts"
HISTORY = f"{LAKE}/card_history"
CHECKPOINT = f"{LAKE}/_checkpoint"
APP = "fraud_stream"
HISTORY_HOURS = 24

HISTORY_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("card_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("amount", DecimalType(18, 2)),
    StructField("channel", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
])


def read_history(spark):
    if DeltaTable.isDeltaTable(spark, HISTORY):
        return spark.read.format("delta").load(HISTORY)
    return spark.createDataFrame([], HISTORY_SCHEMA)


def process_batch(raw, batch_id):
    """Runs once per micro-batch. Safe to re-run with the same batch_id after a crash."""
    spark = raw.sparkSession
    raw = raw.withColumn("ingested_at", F.current_timestamp()).persist()

    if raw.isEmpty():
        raw.unpersist()
        return

    # 1. Bronze: raw lines exactly as received
    append_idempotent(
        raw, BRONZE, f"{APP}-bronze", batch_id
    )

    # 2. Quality gate
    valid, rejected = parse_and_validate(raw)
    rejected = rejected.withColumn(
        "quarantined_at", F.current_timestamp()
    ).persist()
    n_rejected = rejected.count()

    append_idempotent(
        rejected, QUARANTINE, f"{APP}-quarantine", batch_id
    )

    # 3. De-duplicate: inside this batch AND against payments already processed
    history = read_history(spark)

    new = (
        valid.dropDuplicates(["transaction_id"])
        .join(
            history.select("transaction_id"),
            "transaction_id",
            "left_anti",
        )
        .persist()
    )

    n_new = new.count()  # Materialise before the history table changes
    newest =new.agg(F.max("event_time")).first()[0] if n_new else None

    n_alerts = 0

    if n_new:
        # 4. Features (batch + card history) and rules
        scored = (
            apply_rules(compute_features(new, CUSTOMERS, history))
            .withColumn("event_date", F.to_date("event_time"))
            .persist()
        )
        scored.count()

        # 5. Silver + gold (MERGE = no duplicates on retry)
        merge_insert_new(
            scored,
            SILVER,
            ["event_date", "transaction_id"],
            partition_by=["event_date"],
        )

        alerts = build_alerts(scored)
        n_alerts = alerts.count()
        merge_insert_new(alerts, GOLD, ["alert_id"])

        # 6. Remember these payments for the next micro-batches,
        # then forget anything older than 24 hours
        append_idempotent(
            new.select(*TIMELINE_COLUMNS),
            HISTORY,
            f"{APP}-history",
            batch_id,
        )

        cutoff = newest - timedelta(hours=HISTORY_HOURS)

        DeltaTable.forPath(spark, HISTORY).delete(
            F.col("event_time") < F.lit(cutoff)
        )

        scored.unpersist()

    print(
        f"micro-batch {batch_id}: raw={raw.count()} "
        f"rejected={n_rejected} new={n_new} alerts={n_alerts}",
        flush=True,
    )

    for df in (raw, rejected, new):
        df.unpersist()


spark = get_spark("fraud-stream")

CUSTOMERS = spark.read.json(
    "data/reference/customers.json"
).cache()

stream = (
    spark.readStream.format("text")
    .option("maxFilesPerTrigger", 1)
    .load("data/landing")
    .where(F.trim("value") != "")
    .withColumn("source_file", F.input_file_name())
)

query = (
    stream.writeStream.queryName(APP)
    .option("checkpointLocation", CHECKPOINT)
    .foreachBatch(process_batch)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

print("\n===== Stream lake totals =====")

for name, path in [
    ("Bronze", BRONZE),
    ("Silver", SILVER),
    ("Quarantine", QUARANTINE),
    ("Gold alerts", GOLD),
]:
    count = (
        spark.read.format("delta").load(path).count()
        if DeltaTable.isDeltaTable(spark, path)
        else 0
    )
    print(f"{name:<12} {count}")

spark.stop()
