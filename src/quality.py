"""Parse raw JSON payments and split them into valid rows and quarantined rows."""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# The expected shape of one payment. Money is DECIMAL, never float (floats cause rounding errors).
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("card_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("event_time", StringType()),
    StructField("amount", DecimalType(18, 2)),
    StructField("currency", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_category", StringType()),
    StructField("merchant_country", StringType()),
    StructField("channel", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
])

ALLOWED_CURRENCIES = ["EUR", "GBP", "USD"]
ALLOWED_CHANNELS = ["POS", "ECOM", "ATM"]
CARD_PRESENT = ["POS", "ATM"]


def rejection_reason():
    """Checks run top to bottom; the first one that fails is the reason. NULL means the row is valid."""
    c = F.col
    return (
        F.when(
            c("transaction_id").isNull()
            & c("card_id").isNull()
            & c("amount").isNull(),
            "malformed_record",
        )
        .when(c("transaction_id").isNull(), "missing_transaction_id")
        .when(c("card_id").isNull(), "missing_card_id")
        .when(c("event_time").isNull(), "invalid_event_time")
        .when(c("amount").isNull() | (c("amount") <= 0), "invalid_amount")
        .when(
            c("currency").isNull()
            | ~c("currency").isin(*ALLOWED_CURRENCIES),
            "unsupported_currency",
        )
        .when(
            c("channel").isNull()
            | ~c("channel").isin(*ALLOWED_CHANNELS),
            "invalid_channel",
        )
        .when(
            c("channel").isin(*CARD_PRESENT)
            & (c("latitude").isNull() | c("longitude").isNull()),
            "missing_location",
        )
    )


def parse_and_validate(raw):
    """raw has a 'value' column (the JSON text). Returns (valid, rejected)."""
    parsed = raw.withColumn(
        "p", F.from_json("value", TRANSACTION_SCHEMA)
    )

    flat = parsed.select(
        "value",
        "source_file",
        "p.*",
    ).withColumn(
        "event_time",
        F.to_timestamp("event_time"),
    )

    checked = flat.withColumn(
        "rejection_reason",
        rejection_reason(),
    )

    valid = checked.where(
        F.col("rejection_reason").isNull()
    ).drop("rejection_reason", "value")

    rejected = (
        checked.where(F.col("rejection_reason").isNotNull())
        .select("value", "source_file", "rejection_reason")
    )

    return valid, rejected
