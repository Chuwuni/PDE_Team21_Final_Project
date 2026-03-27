from __future__ import annotations
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "warehouse_db"
SCHEMA_NAME = "dwh"
RAW_PATH = "s3a://datalake/raw_data"

JDBC_CONFIG = {
    "url": "jdbc:postgresql://postgres:5432/warehouse_db",
    "user": "warehouse_user",
    "password": "warehouse_password",
    "driver": "org.postgresql.Driver",
    "batchsize": "5000",
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


def _get_spark_session():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("dwh-full-load")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123456")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars", _spark_jars())
        .getOrCreate()
    )


def prepare_schema():
    base_dir = Path(__file__).resolve().parent
    ddl_path = base_dir / "table_creation_script.sql"
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    if ddl_path.exists():
        hook.run(ddl_path.read_text(encoding="utf-8"))

    tables = [
        "order_driver_assignments",
        "order_items",
        "orders",
        "products",
        "stores",
        "drivers",
        "users",
    ]
    truncate_sql = (
        f"TRUNCATE TABLE {', '.join([f'{SCHEMA_NAME}.{t}' for t in tables])} CASCADE;"
    )
    hook.run(truncate_sql)


def load_table_to_dwh(table_type: str):
    from pyspark.sql import functions as F, Window

    spark = _get_spark_session()
    try:
        raw_df = spark.read.parquet(RAW_PATH)

        if table_type == "users":
            df = (
                raw_df.filter(F.col("user_id").isNotNull())
                .select("user_id", "user_phone")
                .dropDuplicates(["user_id"])
            )

        elif table_type == "drivers":
            df = (
                raw_df.filter(F.col("driver_id").isNotNull())
                .select("driver_id", "driver_phone")
                .dropDuplicates(["driver_id"])
            )

        elif table_type == "stores":
            df = (
                raw_df.filter(F.col("store_id").isNotNull())
                .select("store_id", "store_address")
                .dropDuplicates(["store_id"])
            )

        elif table_type == "products":
            df = (
                raw_df.filter(F.col("item_id").isNotNull())
                .select("item_id", "item_title", "item_category")
                .dropDuplicates(["item_id"])
            )

        elif table_type == "orders":
            w = Window.partitionBy("order_id").orderBy(
                F.col("created_at").desc_nulls_last()
            )
            df = (
                raw_df.filter(F.col("order_id").isNotNull())
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

        elif table_type == "order_items":
            w = Window.partitionBy("order_id", "item_id").orderBy(
                F.col("created_at").asc_nulls_last()
            )
            df = (
                raw_df.filter(
                    F.col("order_id").isNotNull() & F.col("item_id").isNotNull()
                )
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

        elif table_type == "order_driver_assignments":
            w = Window.partitionBy(
                "order_id", "driver_id", "delivery_started_at"
            ).orderBy(F.col("created_at").desc())
            df = (
                raw_df.filter(
                    F.col("order_id").isNotNull() & F.col("driver_id").isNotNull()
                )
                .withColumn("rn", F.row_number().over(w))
                .filter(F.col("rn") == 1)
                .withColumn(
                    "assignment_id",
                    F.concat_ws(
                        "_",
                        "order_id",
                        "driver_id",
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

        df.write.format("jdbc").options(**JDBC_CONFIG).option(
            "dbtable", f"{SCHEMA_NAME}.{table_type}"
        ).mode("append").save()

    finally:
        spark.stop()


def validate_load():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
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
        count = hook.get_first(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{t}")[0]
        if count == 0:
            raise ValueError(f"Table {SCHEMA_NAME}.{t} is empty after load!")


with DAG(
    dag_id="s3_postgres_full_load",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    t_start = PythonOperator(task_id="prepare_schema", python_callable=prepare_schema)
    t_end = PythonOperator(task_id="validate_load", python_callable=validate_load)

    all_tasks = {}

    dimension_tables = ["users", "drivers", "stores", "products"]
    for table in dimension_tables:
        all_tasks[table] = PythonOperator(
            task_id=f"load_{table}",
            python_callable=load_table_to_dwh,
            op_kwargs={"table_type": table},
        )

    fact_tables = ["orders", "order_items", "order_driver_assignments"]
    for table in fact_tables:
        all_tasks[table] = PythonOperator(
            task_id=f"load_{table}",
            python_callable=load_table_to_dwh,
            op_kwargs={"table_type": table},
        )

    t_start >> [
        all_tasks["users"],
        all_tasks["drivers"],
        all_tasks["stores"],
        all_tasks["products"],
    ]

    [
        all_tasks["users"],
        all_tasks["drivers"],
        all_tasks["stores"],
        all_tasks["products"],
    ] >> all_tasks["orders"]

    all_tasks["orders"] >> [
        all_tasks["order_items"],
        all_tasks["order_driver_assignments"],
    ]

    [all_tasks["order_items"], all_tasks["order_driver_assignments"]] >> t_end
