from pyspark.sql import functions as F

from delta_utils import merge_insert_new
from features import compute_features
from rules import apply_rules, build_alerts
from spark_session import get_spark

GOLD = "data/lake/gold/fraud_alerts"

spark = get_spark("lesson8-score")

silver = spark.read.format("delta").load(
    "data/lake/silver/transactions"
)
customers = spark.read.json("data/reference/customers.json")
labels = spark.read.json("data/reference/fraud_labels.json")

scored = apply_rules(
    compute_features(silver, customers)
).cache()

# GOLD: write alerts (MERGE on alert_id, so running twice adds nothing)
merge_insert_new(build_alerts(scored), GOLD, keys=["alert_id"])
gold = spark.read.format("delta").load(GOLD)

print(f"\n===== Gold alerts in table: {gold.count()}")

print("\nTop 5 alerts (what a fraud analyst sees):")
(
    gold.orderBy(F.desc("risk_score"), "event_time")
    .select(
        "event_time",
        "amount",
        "merchant_country",
        "risk_score",
        "triggered_rules",
    )
    .show(5, truncate=False)
)

# ACCURACY: compare alerts with the delayed fraud labels
df = scored.join(labels, "transaction_id", "left").fillna(
    {"is_fraud": False}
)

m = df.agg(
    F.sum(
        (F.col("is_alert") & F.col("is_fraud")).cast("int")
    ).alias("tp"),
    F.sum(
        (F.col("is_alert") & ~F.col("is_fraud")).cast("int")
    ).alias("fp"),
    F.sum(
        (~F.col("is_alert") & F.col("is_fraud")).cast("int")
    ).alias("fn"),
).first()

tp, fp, fn = m["tp"] or 0, m["fp"] or 0, m["fn"] or 0

precision = tp / (tp + fp) if tp + fp else 0.0
recall = tp / (tp + fn) if tp + fn else 0.0

print("===== Detection quality =====")
print(
    f"Alerts raised: {tp + fp} "
    f"(real fraud: {tp}, false alarms: {fp})"
)
print(f"Missed fraud payments: {fn}")
print(
    f"Precision: {precision:.0%} "
    "<- of our alerts, how many were really fraud"
)
print(
    f"Recall: {recall:.0%} "
    "<- of all fraud payments, how many we caught"
)

print("\nRecall by fraud pattern:")
(
    df.where("is_fraud")
    .groupBy("fraud_pattern")
    .agg(
        F.count("*").alias("fraud_payments"),
        F.sum(F.col("is_alert").cast("int")).alias("alerted"),
    )
    .orderBy("fraud_pattern")
    .show()
)

print("Card level: were the stolen cards caught?")
(
    df.where("is_fraud")
    .groupBy("card_id")
    .agg(
        F.max(F.col("is_alert").cast("int")).alias("caught"),
        F.count("*").alias("compromised_payments"),
    )
    .agg(
        F.count("*").alias("compromised_cards"),
        F.sum("caught").alias("cards_caught"),
    )
    .show()
)

spark.stop()
