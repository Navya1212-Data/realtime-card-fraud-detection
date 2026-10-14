"""Behaviour features per card: how fast, how far, how unusual."""

from pyspark.sql import Window
from pyspark.sql import functions as F

WINDOW_SECONDS = 10 * 60
SMALL_AMOUNT = 2.00
EARTH_RADIUS_KM = 6371.0
MIN_GAP_SECONDS = 60
TIMELINE_COLUMNS = [
    "transaction_id",
    "card_id",
    "event_time",
    "amount",
    "channel",
    "latitude",
    "longitude",
]


def haversine_km(lat1, lon1, lat2, lon2):
    """Straight-line distance in km between two points on Earth."""
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (
        F.sin(dlat / 2) ** 2
        + F.cos(F.radians(lat1))
        * F.cos(F.radians(lat2))
        * F.sin(dlon / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * F.asin(
        F.least(F.lit(1.0), F.sqrt(a))
    )


def compute_features(batch, customers, history=None):
    """Add behaviour features to every payment in batch.

    `history` = earlier payments of the same cards. In streaming (Lesson 9),
    it lets a card's activity carry over from one micro-batch to the next.
    """
    current = batch.select(*TIMELINE_COLUMNS).withColumn(
        "is_current", F.lit(True)
    )
    timeline = current

    if history is not None:
        previous = (
            history.join(
                batch.select("card_id").distinct(),
                "card_id",
                "left_semi",
            )
            .select(*TIMELINE_COLUMNS)
            .withColumn("is_current", F.lit(False))
        )
        timeline = current.unionByName(previous)

    timeline = timeline.withColumn(
        "ts", F.col("event_time").cast("long")
    )

    by_card = Window.partitionBy("card_id").orderBy("ts")
    earlier = by_card.rowsBetween(Window.unboundedPreceding, -1)

    located = (
        F.col("channel").isin("POS", "ATM")
        & F.col("latitude").isNotNull()
    )

    windowed = timeline.select(
        "transaction_id",
        "is_current",
        F.count(F.lit(1))
        .over(by_card.rangeBetween(-WINDOW_SECONDS, 0))
        .alias("txn_count_10m"),
        F.sum(
            F.when(F.col("amount") <= SMALL_AMOUNT, 1).otherwise(0)
        )
        .over(by_card.rangeBetween(-WINDOW_SECONDS, 0))
        .alias("small_txn_count_10m"),
        F.last(
            F.when(located, F.col("latitude")), ignorenulls=True
        )
        .over(earlier)
        .alias("prev_lat"),
        F.last(
            F.when(located, F.col("longitude")), ignorenulls=True
        )
        .over(earlier)
        .alias("prev_lon"),
        F.last(
            F.when(located, F.col("event_time")), ignorenulls=True
        )
        .over(earlier)
        .alias("prev_time"),
    ).where("is_current").drop("is_current")

    profile = customers.select(
        "card_id", "home_country", "avg_amount", "std_amount"
    )

    enriched = batch.join(
        windowed, "transaction_id"
    ).join(
        F.broadcast(profile), "card_id", "left"
    )

    gap_seconds = (
        F.col("event_time").cast("long")
        - F.col("prev_time").cast("long")
    )
    gap_hours = (
        F.greatest(gap_seconds, F.lit(MIN_GAP_SECONDS)) / 3600.0
    )

    distance = F.when(
        located & F.col("prev_lat").isNotNull(),
        haversine_km(
            F.col("prev_lat"),
            F.col("prev_lon"),
            F.col("latitude"),
            F.col("longitude"),
        ),
    )

    zscore = (
        F.col("amount").cast("double") - F.col("avg_amount")
    ) / F.greatest(F.col("std_amount"), F.lit(1.0))

    return (
        enriched
        .withColumn("distance_km", F.round(distance, 1))
        .withColumn(
            "speed_kmh",
            F.round(F.col("distance_km") / gap_hours, 1),
        )
        .withColumn("amount_zscore", F.round(zscore, 2))
        .drop("prev_lat", "prev_lon", "prev_time")
    )
