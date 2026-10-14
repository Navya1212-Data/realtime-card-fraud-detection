from delta.tables import DeltaTable
from pyspark.sql import functions as F

from delta_utils import append_idempotent, merge_insert_new
from quality import parse_and_validate
from spark_session import get_spark

LAKE = "data/lake"
BRONZE = f"{LAKE}/bronze/transactions"
SILVER = f"{LAKE}/silver/transactions"
QUARANTINE = f"{LAKE}/quarantine/transactions"

BATCH_ID = 0  # In Lesson 9, Spark Streaming will give each micro-batch its own number

spark = get_spark("lesson6-medallion")

# BRONZE: every raw line exactly as received, plus where and when it came from
raw = (
    spark.read.text("data/landing")
    .withColumn("source_file", F.input_file_name())
    .withColumn("ingested_at", F.current_timestamp())
)

append_idempotent(raw, BRONZE, app_id="bronze", batch_id=BATCH_ID)

# Split into valid and rejected (your Lesson 5 code)
valid, rejected = parse_and_validate(raw)

# QUARANTINE: bad records with the reason
append_idempotent(
    rejected.withColumn("quarantined_at", F.current_timestamp()),
    QUARANTINE,
    app_id="quarantine",
    batch_id=BATCH_ID,
)

# SILVER: clean payments, de-duplicated, partitioned by day
silver = (
    valid.dropDuplicates(["transaction_id"])
    .withColumn("event_date", F.to_date("event_time"))
)

merge_insert_new(
    silver,
    SILVER,
    keys=["event_date", "transaction_id"],
    partition_by=["event_date"],
)

# Results
print("\n===== Row counts =====")

for name, path in [
    ("Bronze", BRONZE),
    ("Silver", SILVER),
    ("Quarantine", QUARANTINE),
]:
    print(
        f"{name:11} "
        f"{spark.read.format('delta').load(path).count()} rows"
    )

print("\n===== Silver table history (Delta transaction log) =====")

(
    DeltaTable.forPath(spark, SILVER)
    .history()
    .select(
        "version",
        "operation",
        F.col("operationMetrics")["numTargetRowsInserted"].alias("rows_inserted"),
    )
    .orderBy("version")
    .show(truncate=False)
)

print(
    "Time travel: silver at version 0 had",
    spark.read.format("delta")
    .option("versionAsOf", 0)
    .load(SILVER)
    .count(),
    "rows",
)

spark.stop()
