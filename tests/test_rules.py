import json

from features import compute_features
from quality import parse_and_validate
from rules import apply_rules


def event(
    txn_id,
    time,
    amount,
    channel="POS",
    country="GB",
    lat=51.5,
    lon=-0.12,
):
    located = channel in ("POS", "ATM")

    return json.dumps({
        "transaction_id": txn_id,
        "card_id": "card_1",
        "customer_id": "u1",
        "event_time": f"2026-01-15T{time}Z",
        "amount": amount,
        "currency": "EUR",
        "merchant_id": "m1",
        "merchant_category": "5411",
        "merchant_country": country,
        "channel": channel,
        "latitude": lat if located else None,
        "longitude": lon if located else None,
    })


def score(spark, lines):
    raw = spark.createDataFrame(
        [(x, "test.json") for x in lines],
        "value string, source_file string",
    )

    valid, _ = parse_and_validate(raw)

    customers = spark.createDataFrame(
        [("card_1", "GB", 50.0, 20.0)],
        "card_id string, home_country string, "
        "avg_amount double, std_amount double",
    )

    rows = apply_rules(
        compute_features(valid, customers)
    ).collect()

    return {r.transaction_id: r for r in rows}


def test_card_testing_fires_on_third_tiny_payment(spark):
    rows = score(spark, [
        event("t1", "10:00:00", 1.00, channel="ECOM"),
        event("t2", "10:00:30", 1.00, channel="ECOM"),
        event("t3", "10:01:00", 1.00, channel="ECOM"),
    ])

    assert not rows["t1"].is_alert
    assert rows["t3"].is_alert
    assert "card_testing" in rows["t3"].triggered_rules


def test_impossible_travel_london_to_tokyo_in_30_minutes(spark):
    rows = score(spark, [
        event("t1", "10:00:00", 30.0),
        event(
            "t2", "10:30:00", 30.0,
            country="JP", lat=35.68, lon=139.65,
        ),
    ])

    assert not rows["t1"].is_alert
    assert rows["t2"].is_alert
    assert "impossible_travel" in rows["t2"].triggered_rules
    assert rows["t2"].speed_kmh > 900


def test_normal_payment_is_not_an_alert(spark):
    rows = score(spark, [
        event("t1", "10:00:00", 45.0),
    ])

    assert rows["t1"].risk_score == 0
    assert not rows["t1"].is_alert
