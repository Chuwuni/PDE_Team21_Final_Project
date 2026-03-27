from __future__ import annotations
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "warehouse_db"
DWH_SCHEMA = "dwh"
MART_SCHEMA = "mart"

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
        SparkSession.builder.appName("build-data-marts")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.jars", _spark_jars())
        .getOrCreate()
    )


def prepare_marts_schema():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    sql = f"""
    CREATE SCHEMA IF NOT EXISTS {MART_SCHEMA};

    DROP TABLE IF EXISTS {MART_SCHEMA}.mart_products;
    DROP TABLE IF EXISTS {MART_SCHEMA}.mart_orders;

    CREATE TABLE {MART_SCHEMA}.mart_orders (
        order_id BIGINT,
        user_id BIGINT,
        store_id BIGINT,

        order_date DATE,
        order_year INT,
        order_month INT,
        order_day INT,

        city TEXT,
        store_address TEXT,
        delivery_address TEXT,

        payment_type TEXT,
        order_cancellation_reason TEXT,

        created_at TIMESTAMP,
        paid_at TIMESTAMP,
        canceled_at TIMESTAMP,

        first_delivery_started_at TIMESTAMP,
        delivered_at TIMESTAMP,

        order_discount NUMERIC(14,2),
        delivery_cost NUMERIC(14,2),

        total_items_count INT,
        canceled_items_count INT,
        paid_items_count INT,

        items_gross_amount NUMERIC(14,2),
        items_discount_amount NUMERIC(14,2),
        items_turnover_amount NUMERIC(14,2),
        items_canceled_amount NUMERIC(14,2),
        paid_items_amount NUMERIC(14,2),

        turnover_amount NUMERIC(14,2),
        revenue_amount NUMERIC(14,2),
        profit_amount NUMERIC(14,2),

        created_order_flag INT,
        delivered_order_flag INT,
        canceled_order_flag INT,
        canceled_after_delivery_flag INT,
        service_cancel_flag INT,

        assigned_driver_count INT,
        courier_changed_flag INT,
        active_driver_flag INT
    );

    CREATE TABLE {MART_SCHEMA}.mart_products (
        order_item_id TEXT,
        order_id BIGINT,
        user_id BIGINT,
        store_id BIGINT,
        driver_id BIGINT,

        product_id BIGINT,
        replaced_product_id BIGINT,
        product_title TEXT,
        product_category TEXT,

        order_date DATE,
        order_year INT,
        order_month INT,
        order_day INT,

        city TEXT,
        store_address TEXT,

        payment_type TEXT,
        order_cancellation_reason TEXT,

        created_at TIMESTAMP,
        paid_at TIMESTAMP,
        canceled_at TIMESTAMP,
        delivered_at TIMESTAMP,

        item_quantity INT,
        item_canceled_quantity INT,
        item_paid_quantity INT,

        item_price NUMERIC(14,2),
        item_discount NUMERIC(14,2),

        gross_amount NUMERIC(14,2),
        discount_amount NUMERIC(14,2),
        turnover_amount NUMERIC(14,2),
        canceled_amount NUMERIC(14,2),
        revenue_amount NUMERIC(14,2),

        created_order_flag INT,
        delivered_order_flag INT,
        canceled_order_flag INT,
        service_cancel_flag INT,
        item_has_cancellation_flag INT
    );
    """
    hook.run(sql)


def build_mart(table_type: str):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    spark = _get_spark_session()

    try:
        orders_df = (
            spark.read.format("jdbc")
            .options(**JDBC_CONFIG)
            .option("dbtable", f"{DWH_SCHEMA}.orders")
            .load()
        )

        order_items_df = (
            spark.read.format("jdbc")
            .options(**JDBC_CONFIG)
            .option("dbtable", f"{DWH_SCHEMA}.order_items")
            .load()
        )

        assignments_df = (
            spark.read.format("jdbc")
            .options(**JDBC_CONFIG)
            .option("dbtable", f"{DWH_SCHEMA}.order_driver_assignments")
            .load()
        )

        stores_df = (
            spark.read.format("jdbc")
            .options(**JDBC_CONFIG)
            .option("dbtable", f"{DWH_SCHEMA}.stores")
            .load()
        )

        products_df = (
            spark.read.format("jdbc")
            .options(**JDBC_CONFIG)
            .option("dbtable", f"{DWH_SCHEMA}.products")
            .load()
        )

        orders_df = (
            orders_df
            .withColumn("order_date", F.to_date("created_at"))
            .withColumn("order_year", F.year("created_at"))
            .withColumn("order_month", F.month("created_at"))
            .withColumn("order_day", F.dayofmonth("created_at"))
        )

        items_enriched = (
            order_items_df
            .withColumn("item_quantity", F.coalesce(F.col("item_quantity"), F.lit(0)))
            .withColumn("item_canceled_quantity", F.coalesce(F.col("item_canceled_quantity"), F.lit(0)))
            .withColumn("item_discount", F.coalesce(F.col("item_discount"), F.lit(0.0)))
            .withColumn("item_price", F.coalesce(F.col("item_price"), F.lit(0.0)))
            .withColumn("item_paid_quantity", F.col("item_quantity") - F.col("item_canceled_quantity"))
            .withColumn("gross_amount", F.col("item_quantity") * F.col("item_price"))
            .withColumn("discount_amount", F.col("item_quantity") * F.col("item_price") * F.col("item_discount"))
            .withColumn("turnover_amount", F.col("item_quantity") * F.col("item_price") * (F.lit(1.0) - F.col("item_discount")))
            .withColumn("canceled_amount", F.col("item_canceled_quantity") * F.col("item_price") * (F.lit(1.0) - F.col("item_discount")))
            .withColumn("revenue_amount", F.col("item_paid_quantity") * F.col("item_price") * (F.lit(1.0) - F.col("item_discount")))
        )

        items_agg = (
            items_enriched
            .groupBy("order_id")
            .agg(
                F.sum("item_quantity").cast("int").alias("total_items_count"),
                F.sum("item_canceled_quantity").cast("int").alias("canceled_items_count"),
                F.sum("item_paid_quantity").cast("int").alias("paid_items_count"),
                F.round(F.sum("gross_amount"), 2).alias("items_gross_amount"),
                F.round(F.sum("discount_amount"), 2).alias("items_discount_amount"),
                F.round(F.sum("turnover_amount"), 2).alias("items_turnover_amount"),
                F.round(F.sum("canceled_amount"), 2).alias("items_canceled_amount"),
                F.round(F.sum("revenue_amount"), 2).alias("paid_items_amount"),
            )
        )

        assignment_agg = (
            assignments_df
            .groupBy("order_id")
            .agg(
                F.min("delivery_started_at").alias("first_delivery_started_at"),
                F.max("delivered_at").alias("delivered_at"),
                F.countDistinct("driver_id").cast("int").alias("assigned_driver_count"),
            )
        )

        if table_type == "mart_orders":
            mart_orders = (
                orders_df.alias("o")
                .join(
                    stores_df.alias("s"),
                    F.col("o.store_id") == F.col("s.store_id"),
                    "left"
                )
                .join(
                    items_agg.alias("i"),
                    F.col("o.order_id") == F.col("i.order_id"),
                    "left"
                )
                .join(
                    assignment_agg.alias("a"),
                    F.col("o.order_id") == F.col("a.order_id"),
                    "left"
                )
                .select(
                    F.col("o.order_id"),
                    F.col("o.user_id"),
                    F.col("o.store_id"),

                    F.col("o.order_date"),
                    F.col("o.order_year"),
                    F.col("o.order_month"),
                    F.col("o.order_day"),

                    F.trim(F.split(F.col("s.store_address"), ",").getItem(0)).alias("city"),
                    F.col("s.store_address"),
                    F.col("o.address_text").alias("delivery_address"),

                    F.col("o.payment_type"),
                    F.col("o.order_cancellation_reason"),

                    F.col("o.created_at"),
                    F.col("o.paid_at"),
                    F.col("o.canceled_at"),

                    F.col("a.first_delivery_started_at"),
                    F.col("a.delivered_at"),

                    F.round(F.coalesce(F.col("o.order_discount"), F.lit(0.0)), 2).alias("order_discount"),
                    F.round(F.coalesce(F.col("o.delivery_cost"), F.lit(0.0)), 2).alias("delivery_cost"),

                    F.coalesce(F.col("i.total_items_count"), F.lit(0)).cast("int").alias("total_items_count"),
                    F.coalesce(F.col("i.canceled_items_count"), F.lit(0)).cast("int").alias("canceled_items_count"),
                    F.coalesce(F.col("i.paid_items_count"), F.lit(0)).cast("int").alias("paid_items_count"),

                    F.round(F.coalesce(F.col("i.items_gross_amount"), F.lit(0.0)), 2).alias("items_gross_amount"),
                    F.round(F.coalesce(F.col("i.items_discount_amount"), F.lit(0.0)), 2).alias("items_discount_amount"),
                    F.round(F.coalesce(F.col("i.items_turnover_amount"), F.lit(0.0)), 2).alias("items_turnover_amount"),
                    F.round(F.coalesce(F.col("i.items_canceled_amount"), F.lit(0.0)), 2).alias("items_canceled_amount"),
                    F.round(F.coalesce(F.col("i.paid_items_amount"), F.lit(0.0)), 2).alias("paid_items_amount"),

                    F.round(
                        F.coalesce(F.col("i.items_turnover_amount"), F.lit(0.0))
                        - F.coalesce(F.col("o.order_discount"), F.lit(0.0)),
                        2
                    ).alias("turnover_amount"),

                    F.round(
                        F.coalesce(F.col("i.paid_items_amount"), F.lit(0.0))
                        - F.coalesce(F.col("o.order_discount"), F.lit(0.0))
                        + F.coalesce(F.col("o.delivery_cost"), F.lit(0.0)),
                        2
                    ).alias("revenue_amount"),

                    F.round(
                        F.coalesce(F.col("i.paid_items_amount"), F.lit(0.0))
                        - F.coalesce(F.col("o.order_discount"), F.lit(0.0))
                        + F.coalesce(F.col("o.delivery_cost"), F.lit(0.0)),
                        2
                    ).alias("profit_amount"),

                    F.lit(1).alias("created_order_flag"),
                    F.when(F.col("a.delivered_at").isNotNull(), F.lit(1)).otherwise(F.lit(0)).alias("delivered_order_flag"),
                    F.when(F.col("o.canceled_at").isNotNull(), F.lit(1)).otherwise(F.lit(0)).alias("canceled_order_flag"),
                    F.when(
                        F.col("o.canceled_at").isNotNull() & F.col("a.delivered_at").isNotNull(),
                        F.lit(1)
                    ).otherwise(F.lit(0)).alias("canceled_after_delivery_flag"),
                    F.when(
                        F.col("o.order_cancellation_reason").isin("Ошибка приложения", "Проблемы с оплатой"),
                        F.lit(1)
                    ).otherwise(F.lit(0)).alias("service_cancel_flag"),

                    F.coalesce(F.col("a.assigned_driver_count"), F.lit(0)).cast("int").alias("assigned_driver_count"),
                    F.when(F.coalesce(F.col("a.assigned_driver_count"), F.lit(0)) > 1, F.lit(1)).otherwise(F.lit(0)).alias("courier_changed_flag"),
                    F.when(F.coalesce(F.col("a.assigned_driver_count"), F.lit(0)) > 0, F.lit(1)).otherwise(F.lit(0)).alias("active_driver_flag"),
                )
            )

            mart_orders.write.format("jdbc").options(**JDBC_CONFIG).option(
                "dbtable", f"{MART_SCHEMA}.mart_orders"
            ).mode("append").save()

        elif table_type == "mart_products":
            first_driver_window = Window.partitionBy("order_id").orderBy(
                F.col("delivery_started_at").asc_nulls_last(),
                F.col("delivered_at").asc_nulls_last()
            )

            first_driver_df = (
                assignments_df
                .withColumn("rn", F.row_number().over(first_driver_window))
                .filter(F.col("rn") == 1)
                .select(
                    "order_id",
                    "driver_id",
                    "delivered_at",
                )
            )

            mart_products = (
                items_enriched.alias("oi")
                .join(
                    orders_df.alias("o"),
                    F.col("oi.order_id") == F.col("o.order_id"),
                    "left"
                )
                .join(
                    stores_df.alias("s"),
                    F.col("o.store_id") == F.col("s.store_id"),
                    "left"
                )
                .join(
                    products_df.alias("p"),
                    F.col("oi.item_id") == F.col("p.item_id"),
                    "left"
                )
                .join(
                    first_driver_df.alias("fd"),
                    F.col("oi.order_id") == F.col("fd.order_id"),
                    "left"
                )
                .select(
                    F.col("oi.order_item_id"),
                    F.col("oi.order_id"),
                    F.col("o.user_id"),
                    F.col("o.store_id"),
                    F.col("fd.driver_id"),

                    F.col("oi.item_id").alias("product_id"),
                    F.col("oi.item_replaced_id").alias("replaced_product_id"),
                    F.col("p.item_title").alias("product_title"),
                    F.col("p.item_category").alias("product_category"),

                    F.col("o.order_date"),
                    F.col("o.order_year"),
                    F.col("o.order_month"),
                    F.col("o.order_day"),

                    F.trim(F.split(F.col("s.store_address"), ",").getItem(0)).alias("city"),
                    F.col("s.store_address"),

                    F.col("o.payment_type"),
                    F.col("o.order_cancellation_reason"),

                    F.col("o.created_at"),
                    F.col("o.paid_at"),
                    F.col("o.canceled_at"),
                    F.col("fd.delivered_at"),

                    F.col("oi.item_quantity").cast("int"),
                    F.col("oi.item_canceled_quantity").cast("int"),
                    F.col("oi.item_paid_quantity").cast("int"),

                    F.round(F.col("oi.item_price"), 2).alias("item_price"),
                    F.round(F.col("oi.item_discount"), 2).alias("item_discount"),

                    F.round(F.col("oi.gross_amount"), 2).alias("gross_amount"),
                    F.round(F.col("oi.discount_amount"), 2).alias("discount_amount"),
                    F.round(F.col("oi.turnover_amount"), 2).alias("turnover_amount"),
                    F.round(F.col("oi.canceled_amount"), 2).alias("canceled_amount"),
                    F.round(F.col("oi.revenue_amount"), 2).alias("revenue_amount"),

                    F.lit(1).alias("created_order_flag"),
                    F.when(F.col("fd.delivered_at").isNotNull(), F.lit(1)).otherwise(F.lit(0)).alias("delivered_order_flag"),
                    F.when(F.col("o.canceled_at").isNotNull(), F.lit(1)).otherwise(F.lit(0)).alias("canceled_order_flag"),
                    F.when(
                        F.col("o.order_cancellation_reason").isin("Ошибка приложения", "Проблемы с оплатой"),
                        F.lit(1)
                    ).otherwise(F.lit(0)).alias("service_cancel_flag"),
                    F.when(F.col("oi.item_canceled_quantity") > 0, F.lit(1)).otherwise(F.lit(0)).alias("item_has_cancellation_flag"),
                )
            )

            mart_products.write.format("jdbc").options(**JDBC_CONFIG).option(
                "dbtable", f"{MART_SCHEMA}.mart_products"
            ).mode("append").save()

        else:
            raise ValueError(f"Unknown mart type: {table_type}")

    finally:
        spark.stop()


def validate_marts():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    tables = ["mart_orders", "mart_products"]
    for table_name in tables:
        count = hook.get_first(f"SELECT COUNT(*) FROM {MART_SCHEMA}.{table_name}")[0]
        if count == 0:
            raise ValueError(f"Table {MART_SCHEMA}.{table_name} is empty after load!")


with DAG(
    dag_id="build_data_marts",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    t_prepare = PythonOperator(
        task_id="prepare_marts_schema",
        python_callable=prepare_marts_schema,
    )

    t_build_orders = PythonOperator(
        task_id="build_mart_orders",
        python_callable=build_mart,
        op_kwargs={"table_type": "mart_orders"},
    )

    t_build_products = PythonOperator(
        task_id="build_mart_products",
        python_callable=build_mart,
        op_kwargs={"table_type": "mart_products"},
    )

    t_validate = PythonOperator(
        task_id="validate_marts",
        python_callable=validate_marts,
    )

    t_prepare >> t_build_orders >> t_build_products >> t_validate