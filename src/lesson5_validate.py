from pyspark.sql import functions as F

from quality import parse_and_validate
from spark_session import get_spark

spark = get_spark("lesson5-validate")

raw = spark.read.text("data/landing").withColumn(
    "source_file", F.input_file_name()
)

valid, rejected = parse_and_validate(raw)

print(f"Raw lines: {raw.count()}")
print(f"Valid:     {valid.count()}")
print(f"Rejected:  {rejected.count()}")

print("Why records were rejected:")
rejected.groupBy("rejection_reason").count().orderBy(
    "rejection_reason"
).show(truncate=False)

print("Sample of valid payments:")
valid.select(
    "card_id",
    "event_time",
    "amount",
    "channel",
    "merchant_country",
).show(5, truncate=False)

spark.stop()
