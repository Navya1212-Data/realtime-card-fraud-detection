"""Explainable fraud rules: each rule that fires adds points; 50+ points = alert."""

from pyspark.sql import functions as F

ALERT_THRESHOLD = 50
RULES_VERSION = "v1"
HIGH_RISK_MCC = ["5732", "5944", "4829", "7995"]  # electronics, jewellery, money transfer, gambling


def rule_conditions():
    """name -> (condition, points)"""
    c = F.col
    return {
        "velocity": (c("txn_count_10m") > 5, 50),
        "card_testing": (c("small_txn_count_10m") >= 3, 55),
        "impossible_travel": (
            (c("distance_km") >= 100) & (c("speed_kmh") > 900),
            50,
        ),
        "amount_anomaly": (
            (c("amount") >= 200) & (c("amount_zscore") >= 4),
            30,
        ),
        "high_risk_mcc": (
            c("merchant_category").isin(*HIGH_RISK_MCC)
            & (c("amount") >= c("avg_amount") * 3),
            20,
        ),
        "foreign_card_present": (
            c("channel").isin("POS", "ATM")
            & (c("merchant_country") != c("home_country")),
            15,
        ),
    }


def apply_rules(features):
    """Add risk_score, triggered_rules (the explanation) and is_alert."""
    rules = {
        name: (F.coalesce(cond, F.lit(False)), pts)
        for name, (cond, pts) in rule_conditions().items()
    }

    score = sum(
        (
            F.when(hit, pts).otherwise(0)
            for hit, pts in rules.values()
        ),
        F.lit(0),
    )

    triggered = F.array_compact(
        F.array(
            *[
                F.when(hit, F.lit(name))
                for name, (hit, _) in rules.items()
            ]
        )
    )

    return (
        features
        .withColumn("risk_score", score.cast("int"))
        .withColumn("triggered_rules", triggered)
        .withColumn("is_alert", F.col("risk_score") >= ALERT_THRESHOLD)
        .withColumn("rules_version", F.lit(RULES_VERSION))
    )


def build_alerts(scored):
    """Gold-layer alerts for the fraud operations team."""
    return scored.where("is_alert").select(
        # Same payment -> same alert_id every time, so re-runs never create duplicate alerts
        F.concat(
            F.lit("alrt_"),
            F.substring(F.sha2("transaction_id", 256), 1, 24),
        ).alias("alert_id"),
        "transaction_id",
        "card_id",
        "customer_id",
        "event_time",
        "amount",
        "merchant_category",
        "merchant_country",
        "channel",
        "risk_score",
        "triggered_rules",
        "rules_version",
        F.lit("OPEN").alias("alert_status"),
    )
