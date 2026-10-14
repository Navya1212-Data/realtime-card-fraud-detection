
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Start Spark (the engine) with Delta Lake support
builder = (
    SparkSession.builder.appName("hello-spark")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Some fake card payments
payments = [
    ("t1", "card_A", 25.50, "GBP"),
    ("t2", "card_A", 1.00, "GBP"),
    ("t3", "card_A", 1.00, "GBP"),
    ("t4", "card_B", 899.99, "EUR"),
    ("t5", "card_C", 12.75, "GBP"),
]

df = spark.createDataFrame(
    payments,
    ["transaction_id", "card_id", "amount", "currency"]
)

print("All payments:")
df.show()

# 3. Summarise per card: how many payments and how much spent
summary = df.groupBy("card_id").agg(
    F.count("*").alias("txn_count"),
    F.round(F.sum("amount"), 2).alias("total_amount"),
)

print("Per-card summary:")
summary.show()

# 4. Save as a Delta table, then read it back
df.write.format("delta").mode("overwrite").save("data/hello_delta")

print("Read back from Delta:")
spark.read.format("delta").load("data/hello_delta").show()

spark.stop()
