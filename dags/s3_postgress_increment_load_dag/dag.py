from __future__ import annotations
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "warehouse_db"
DWH_SCHEMA = "dwh"
STAGING_SCHEMA = "staging"
RAW_PATH = "s3a://datalake/raw_data"

JDBC_CONFIG = {
    "url": "jdbc:postgresql://postgres:5432/warehouse_db",
    "user": "warehouse_user",
    "password": "warehouse_password",
    "driver": "org.postgresql.Driver",
}

MINIO_CONF = {
    "endpoint": "http://minio:9000",
    "access_key": "minio",
    "secret_key": "minio123456",
}

SPARK_JARS = [
    "hadoop-aws-3.4.2.jar",
    "bundle-2.29.52.jar",
    "postgresql-42.7.3.jar",
]


def _spark_jars():
    import os
    import pyspark

    jars_dir = os.path.join(os.path.dirname(pyspark.__file__), "jars")
    return ",".join(os.path.join(jars_dir, jar_name) for jar_name in SPARK_JARS)


def _base_spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("incremental_load")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars", _spark_jars())
        .getOrCreate()
    )


def _save_stg(df, table):
    df.write.format("jdbc").options(**JDBC_CONFIG).option(
        "dbtable", f"{STAGING_SCHEMA}.{table}"
    ).mode("append").save()


def _upsert(table, pk, cols):
    upd = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols])
    sql = f"INSERT INTO {DWH_SCHEMA}.{table} SELECT * FROM {STAGING_SCHEMA}.{table} ON CONFLICT ({pk}) DO UPDATE SET {upd};"
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(sql)
    hook.run(f"TRUNCATE TABLE {STAGING_SCHEMA}.{table};")


def load_inc(table_type, **context):
    from pyspark.sql import functions as F, Window

    spark = _base_spark()
    start, end = context["data_interval_start"], context["data_interval_end"]
    try:
        raw = spark.read.parquet(RAW_PATH).filter(
            (F.col("created_at") >= start) & (F.col("created_at") < end)
        )

        if table_type == "users":
            df = (
                raw.filter(F.col("user_id").isNotNull())
                .select("user_id", "user_phone")
                .dropDuplicates(["user_id"])
            )
        elif table_type == "drivers":
            df = (
                raw.filter(F.col("driver_id").isNotNull())
                .select("driver_id", "driver_phone")
                .dropDuplicates(["driver_id"])
            )
        elif table_type == "stores":
            df = (
                raw.filter(F.col("store_id").isNotNull())
                .select("store_id", "store_address")
                .dropDuplicates(["store_id"])
            )
        elif table_type == "products":
            df = (
                raw.filter(F.col("item_id").isNotNull())
                .select("item_id", "item_title", "item_category")
                .dropDuplicates(["item_id"])
            )
        elif table_type == "orders":
            w = Window.partitionBy("order_id").orderBy(F.col("created_at").desc())
            df = (
                raw.filter(F.col("order_id").isNotNull())
                .withColumn("rn", F.row_number().over(w))
                .filter(F.col("rn") == 1)
                .select(
                    "order_id",
                    "user_id",
                    "store_id",
                    "address_text",
                    "created_at",
                    "paid_at",
                    "canceled_at",
                    "payment_type",
                    "order_discount",
                    "order_cancellation_reason",
                    "delivery_cost",
                )
            )
        elif table_type == "items":
            w = Window.partitionBy("order_id", "item_id").orderBy(
                F.col("created_at").asc()
            )
            df = (
                raw.filter(F.col("order_id").isNotNull() & F.col("item_id").isNotNull())
                .withColumn("row_num", F.row_number().over(w))
                .withColumn(
                    "order_item_id", F.concat_ws("_", "order_id", "item_id", "row_num")
                )
                .select(
                    "order_item_id",
                    "order_id",
                    "item_id",
                    "item_quantity",
                    "item_price",
                    "item_canceled_quantity",
                    "item_discount",
                    "item_replaced_id",
                )
            )
        elif table_type == "assignments":
            w = Window.partitionBy(
                "order_id", "driver_id", "delivery_started_at"
            ).orderBy(F.col("created_at").desc())
            df = (
                raw.filter(
                    F.col("order_id").isNotNull() & F.col("driver_id").isNotNull()
                )
                .withColumn("rn", F.row_number().over(w))
                .filter(F.col("rn") == 1)
                .withColumn(
                    "assignment_id",
                    F.concat_ws(
                        "_",
                        F.col("order_id"),
                        F.col("driver_id"),
                        F.coalesce(
                            F.col("delivery_started_at").cast("string"),
                            F.lit("no_start"),
                        ),
                    ),
                )
                .select(
                    "assignment_id",
                    "order_id",
                    "driver_id",
                    "delivery_started_at",
                    "delivered_at",
                )
            )

        target = (
            table_type
            if table_type not in ["items", "assignments"]
            else (
                "order_items" if table_type == "items" else "order_driver_assignments"
            )
        )
        _save_stg(df, target)
    finally:
        spark.stop()


def prepare():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};")
    tables = [
        "users",
        "drivers",
        "stores",
        "products",
        "orders",
        "order_items",
        "order_driver_assignments",
    ]
    for t in tables:
        hook.run(
            f"CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.{t} (LIKE {DWH_SCHEMA}.{t} INCLUDING ALL);"
        )
        hook.run(f"TRUNCATE TABLE {STAGING_SCHEMA}.{t};")


with DAG(
    dag_id="s3_postgres_incremental_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
) as dag:
    t_prepare = PythonOperator(task_id="prepare_staging", python_callable=prepare)

    meta = {
        "users": ("user_id", ["user_phone"]),
        "drivers": ("driver_id", ["driver_phone"]),
        "stores": ("store_id", ["store_address"]),
        "products": ("item_id", ["item_title", "item_category"]),
        "orders": (
            "order_id",
            [
                "address_text",
                "paid_at",
                "canceled_at",
                "payment_type",
                "order_discount",
                "order_cancellation_reason",
                "delivery_cost",
            ],
        ),
        "order_items": (
            "order_item_id",
            [
                "item_quantity",
                "item_price",
                "item_canceled_quantity",
                "item_discount",
                "item_replaced_id",
            ],
        ),
        "order_driver_assignments": (
            "assignment_id",
            ["delivery_started_at", "delivered_at"],
        ),
    }

    all_loads = []

    for target_table, (pk, columns) in meta.items():
        t_type = (
            "items"
            if target_table == "order_items"
            else (
                "assignments"
                if target_table == "order_driver_assignments"
                else target_table
            )
        )

        t_load = PythonOperator(
            task_id=f"load_{target_table}_to_stg",
            python_callable=load_inc,
            op_kwargs={"table_type": t_type},
        )

        t_upsert = PythonOperator(
            task_id=f"upsert_{target_table}",
            python_callable=_upsert,
            op_kwargs={"table": target_table, "pk": pk, "cols": columns},
        )

        t_prepare >> t_load >> t_upsert
