from delta_utils import append_idempotent, merge_insert_new


def test_merge_twice_does_not_duplicate(spark, tmp_path):
    path = str(tmp_path / "silver")

    df = spark.createDataFrame(
        [(1, "a"), (2, "b")],
        "id int, v string",
    )

    merge_insert_new(df, path, keys=["id"])
    merge_insert_new(df, path, keys=["id"])  # Retry

    assert (
        spark.read.format("delta").load(path).count()
        == 2
    )


def test_retried_batch_is_skipped(spark, tmp_path):
    path = str(tmp_path / "bronze")

    df = spark.createDataFrame(
        [(1, "a"), (2, "b")],
        "id int, v string",
    )

    append_idempotent(
        df, path, app_id="app", batch_id=0
    )

    # Retrying the same batch must not add rows.
    append_idempotent(
        df, path, app_id="app", batch_id=0
    )

    assert (
        spark.read.format("delta").load(path).count()
        == 2
    )

    # A new batch should add the rows.
    append_idempotent(
        df, path, app_id="app", batch_id=1
    )

    assert (
        spark.read.format("delta").load(path).count()
        == 4
    )
