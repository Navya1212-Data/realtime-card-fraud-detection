"""Delta Lake helpers that make every write safe to re-run (no duplicates)."""

from delta.tables import DeltaTable


def append_idempotent(df, path, app_id, batch_id, partition_by=None):
    """Append, but Delta skips the write if this app_id + batch_id was already committed."""
    writer = (
        df.write.format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", batch_id)
    )

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(path)


def merge_insert_new(df, path, keys, partition_by=None):
    """Insert only rows whose keys are not already in the table (an 'upsert' without updates)."""
    spark = df.sparkSession

    if not DeltaTable.isDeltaTable(spark, path):
        writer = df.limit(0).write.format("delta")

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(path)

    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)

    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(df.alias("s"), condition)
        .whenNotMatchedInsertAll()
        .execute()
    )
