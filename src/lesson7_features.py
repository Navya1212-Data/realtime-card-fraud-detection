from pyspark.sql import functions as F

from features import compute_features
from spark_session import get_spark

spark = get_spark("lesson7-features")

silver = spark.read.format("delta").load(
    "data/lake/silver/transactions"
)
customers = spark.read.json("data/reference/customers.json")
labels = spark.read.json("data/reference/fraud_labels.json")

features = compute_features(silver, customers)

# Labels are used ONLY here, to check the features make sense -
# never inside the pipeline.
checked = features.join(
    labels, "transaction_id", "left"
).withColumn(
    "pattern",
    F.coalesce(F.col("fraud_pattern"), F.lit("normal"))
)

print("Feature averages: each fraud pattern vs normal payments")
(
    checked.groupBy("pattern")
    .agg(
        F.count("*").alias("payments"),
        F.round(F.avg("txn_count_10m"), 1).alias("avg_txn_10m"),
        F.round(F.avg("small_txn_count_10m"), 1).alias("avg_small_10m"),
        F.round(F.max("speed_kmh"), 0).alias("max_speed_kmh"),
        F.round(F.avg("amount_zscore"), 1).alias("avg_zscore"),
    )
    .orderBy("pattern")
    .show(truncate=False)
)

print("One card-testing attack, payment by payment:")
victim = (
    checked.where("pattern = 'card_testing'")
    .select("card_id")
    .first()["card_id"]
)

(
    checked.where(F.col("card_id") == victim)
    .orderBy("event_time")
    .select(
        "event_time",
        "amount",
        "channel",
        "txn_count_10m",
        "small_txn_count_10m",
        "pattern",
    )
    .show(truncate=False)
)

spark.stop()
