"""Generate fake card payments with hidden fraud patterns.

Writes:
    data/landing/transactions_00000.json ... -> the live payment stream (one JSON per line)
    data/reference/customers.json             -> each customer's normal spending profile
    data/reference/fraud_labels.json          -> which payments were fraud (like a bank's chargeback feed)
"""

import json
import os
import random
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

SEED = 42  # same seed = same data every run, so results are repeatable
N_CUSTOMERS = 200
MINUTES = 120
FRAUD_RATE = 0.05
EVENTS_PER_FILE = 200

HOME_CITIES = [
    ("GB", "London", 51.5074, -0.1278),
    ("IE", "Dublin", 53.3498, -6.2603),
    ("NL", "Amsterdam", 52.3676, 4.9041),
    ("DE", "Berlin", 52.5200, 13.4050),
    ("FR", "Paris", 48.8566, 2.3522),
]

FAR_CITIES = [
    ("US", "New York", 40.7128, -74.0060),
    ("SG", "Singapore", 1.3521, 103.8198),
    ("AE", "Dubai", 25.2048, 55.2708),
    ("JP", "Tokyo", 35.6762, 139.6503),
]

EVERYDAY_MCC = ["5411", "5812", "5814", "5541", "4111", "5912"]
# groceries, restaurants, fuel, transport...

HIGH_RISK_MCC = ["5732", "5944", "4829", "7995"]
# electronics, jewellery, money transfer, gambling

rng = random.Random(SEED)
START = datetime(2026, 1, 15, 8, 0, tzinfo=timezone.utc)


def token(prefix, length=16):
    return f"{prefix}_{uuid.UUID(int=rng.getrandbits(128)).hex[:length]}"


def at(minutes):
    return START + timedelta(minutes=minutes)


def near(lat, lon, jitter=0.05):
    return (
        lat + rng.uniform(-jitter, jitter),
        lon + rng.uniform(-jitter, jitter),
    )


def make_customers():
    customers = []

    for i in range(1, N_CUSTOMERS + 1):
        country, city, lat, lon = rng.choice(HOME_CITIES)
        avg = round(rng.uniform(20, 120), 2)

        customers.append({
            "customer_id": f"cust_{i:05d}",
            "card_id": token("card"),  # tokenised card: no real card number
            "home_country": country,
            "home_city": city,
            "home_lat": lat,
            "home_lon": lon,
            "avg_amount": avg,
            "std_amount": round(avg * rng.uniform(0.3, 0.6), 2),
        })

    return customers


def txn(c, ts, amount, mcc, channel, country=None, location=None):
    if channel in ("POS", "ATM"):  # in-person payments have a location
        lat, lon = location or near(c["home_lat"], c["home_lon"])
    else:
        lat, lon = None, None

    return {
        "transaction_id": token("txn", 20),
        "card_id": c["card_id"],
        "customer_id": c["customer_id"],
        "event_time": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "amount": round(amount, 2),
        "currency": "EUR",
        "merchant_id": f"mer_{rng.randint(1, 5000):05d}",
        "merchant_category": mcc,
        "merchant_country": country or c["home_country"],
        "channel": channel,
        "latitude": round(lat, 5) if lat is not None else None,
        "longitude": round(lon, 5) if lon is not None else None,
    }


def normal_activity(c):
    out = []
    n = rng.randint(1, MINUTES // 15)

    for offset in sorted(rng.uniform(0, MINUTES) for _ in range(n)):
        channel = rng.choices(
            ["POS", "ECOM", "ATM"], weights=[70, 25, 5]
        )[0]

        amount = max(
            3.0,
            rng.gauss(c["avg_amount"], c["std_amount"])
        )

        if channel == "ATM":
            amount = max(20.0, round(amount / 20) * 20)

        out.append((
            txn(c, at(offset), amount, rng.choice(EVERYDAY_MCC), channel),
            None
        ))

    return out


def card_testing(c, t0):
    """Fraudster tests a stolen card with tiny online payments, then makes a big purchase."""
    out, t = [], at(t0)

    for _ in range(rng.randint(4, 6)):
        out.append((
            txn(c, t, rng.uniform(0.5, 1.99), "5815", "ECOM", "US"),
            "card_testing"
        ))
        t += timedelta(seconds=rng.randint(15, 45))

    out.append((
        txn(c, t + timedelta(seconds=60),
            rng.uniform(800, 2500), "5732", "ECOM", "US"),
        "card_testing"
    ))

    return out


def impossible_travel(c, t0):
    """A genuine payment at home, then the cloned card is used on another continent minutes later."""
    out = [
        (txn(c, at(t0), c["avg_amount"],
             rng.choice(EVERYDAY_MCC), "POS"), None)
    ]

    country, _, lat, lon = rng.choice(FAR_CITIES)
    t = at(t0 + rng.uniform(20, 60))

    for _ in range(rng.randint(2, 3)):
        e = txn(
            c, t, rng.uniform(150, 900),
            "5944", "POS", country, near(lat, lon)
        )
        out.append((e, "impossible_travel"))
        t += timedelta(minutes=rng.uniform(3, 10))

    return out


def velocity_burst(c, t0):
    """A stolen physical card used for many purchases within a few minutes."""
    out, t = [], at(t0)

    for _ in range(rng.randint(8, 10)):
        mcc = rng.choice(EVERYDAY_MCC + HIGH_RISK_MCC)
        out.append((
            txn(c, t, rng.uniform(40, 250), mcc, "POS"),
            "velocity_burst"
        ))
        t += timedelta(seconds=rng.randint(15, 40))

    return out


def amount_spike(c, t0):
    """One purchase far above the customer's normal spend, at a high-risk merchant."""
    amount = c["avg_amount"] * rng.uniform(12, 20)

    return [(
        txn(c, at(t0), amount, rng.choice(HIGH_RISK_MCC), "POS"),
        "amount_spike"
    )]


def bad_record(kind, c):
    """Broken records that real feeds contain. The pipeline must quarantine these, not crash."""
    e = txn(c, at(rng.uniform(0, MINUTES)), 25.0, "5411", "POS")

    if kind == "malformed_json":
        return json.dumps(e)[:25]

    if kind == "missing_card_id":
        e.pop("card_id")

    elif kind == "negative_amount":
        e["amount"] = -42.5

    elif kind == "unsupported_currency":
        e["currency"] = "XYZ"

    elif kind == "invalid_channel":
        e["channel"] = "FAX"

    return json.dumps(e)


def write_lines(path, lines):
    # Write to a hidden temp file, then rename it.
    # Spark ignores files starting with "." so it never reads a half-written file.
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def main():
    customers = make_customers()

    patterns = {
        "card_testing": card_testing,
        "impossible_travel": impossible_travel,
        "velocity_burst": velocity_burst,
        "amount_spike": amount_spike,
    }

    names = list(patterns)
    victims = rng.sample(
        customers,
        max(4, round(N_CUSTOMERS * FRAUD_RATE))
    )

    labelled = []

    for c in customers:
        labelled.extend(normal_activity(c))

    for i, c in enumerate(victims):
        name = names[i % len(names)]
        labelled.extend(
            patterns[name](c, rng.uniform(5, MINUTES - 15))
        )

    labelled.sort(key=lambda pair: pair[0]["event_time"])

    events = [json.dumps(e) for e, _ in labelled]

    labels = [
        {
            "transaction_id": e["transaction_id"],
            "is_fraud": p is not None,
            "fraud_pattern": p,
        }
        for e, p in labelled
    ]

    bad_kinds = [
        "malformed_json",
        "missing_card_id",
        "negative_amount",
        "unsupported_currency",
        "invalid_channel",
    ]

    for kind in bad_kinds:
        events.insert(
            rng.randrange(len(events) + 1),
            bad_record(kind, rng.choice(customers))
        )

    landing = Path("data/landing")
    n_files = 0

    for i in range(0, len(events), EVENTS_PER_FILE):
        write_lines(
            landing / f"transactions_{n_files:05d}.json",
            events[i:i + EVENTS_PER_FILE]
        )
        n_files += 1

    write_lines(
        Path("data/reference/customers.json"),
        [json.dumps(c) for c in customers]
    )

    write_lines(
        Path("data/reference/fraud_labels.json"),
        [json.dumps(x) for x in labels]
    )

    fraud = Counter(
        x["fraud_pattern"] for x in labels if x["is_fraud"]
    )

    print(f"Customers:       {len(customers)}")
    print(f"Payments:        {len(labels)} (+ {len(bad_kinds)} broken records)")
    print(f"Fraud payments:  {sum(fraud.values())} {dict(fraud)}")
    print(f"Files written:   {n_files} files in landing/")


if __name__ == "__main__":
    main()
