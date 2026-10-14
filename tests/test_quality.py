import json

from quality import parse_and_validate


def event(**overrides):
    e = {
        "transaction_id": "t1",
        "card_id": "c1",
        "customer_id": "u1",
        "event_time": "2026-01-15T10:00:00Z",
        "amount": 25.0,
        "currency": "EUR",
        "merchant_id": "m1",
        "merchant_category": "5411",
        "merchant_country": "GB",
        "channel": "POS",
        "latitude": 51.5,
        "longitude": -0.12,
    }
    e.update(overrides)
    return json.dumps(e)


def test_bad_records_are_quarantined_with_a_reason(spark):
    no_card = json.loads(event(transaction_id="t3"))
    no_card.pop("card_id")

    lines = [
        event(),  # Valid record
        '{"transaction_id": "t2", "ca',  # Malformed JSON
        json.dumps(no_card),  # Missing card_id
        event(transaction_id="t4", amount=-5),  # Negative amount
        event(transaction_id="t5", currency="XYZ"),  # Unsupported currency
        event(transaction_id="t6", channel="FAX"),  # Invalid channel
    ]

    raw = spark.createDataFrame(
        [(x, "test.json") for x in lines],
        "value string, source_file string",
    )

    valid, rejected = parse_and_validate(raw)

    assert [
        r.transaction_id for r in valid.collect()
    ] == ["t1"]

    reasons = sorted(
        r.rejection_reason for r in rejected.collect()
    )

    assert reasons == [
        "invalid_amount",
        "invalid_channel",
        "malformed_record",
        "missing_card_id",
        "unsupported_currency",
    ]
