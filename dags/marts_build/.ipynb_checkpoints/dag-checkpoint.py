from __future__ import annotations
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "warehouse_db"

JDBC_CONFIG = {
    "url": "jdbc:postgresql://postgres:5432/warehouse_db",
    "user": "warehouse_user",
    "password": "warehouse_password",
    "driver": "org.postgresql.Driver",
    "batchsize": "5000",
}


def _get_spark_session():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("build-marts")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )


def prepare_mart_schema():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    sql = """
    CREATE SCHEMA IF NOT EXISTS mart AUTHORIZATION warehouse_user;

    CREATE TABLE IF NOT EXISTS mart.mart_order_delivery (
        order_id BIGINT PRIMARY KEY,
        order_date DATE NOT NULL,
        created_at TIMESTAMP NOT NULL,
        user_id BIGINT NOT NULL,
        store_id BIGINT NOT NULL,
        driver_id BIGINT,
        payment_type TEXT,
        items_count BIGINT,
        gross_amount NUMERIC(14, 2),
        item_discount_sum NUMERIC(14, 2),
        order_discount NUMERIC(14, 2),
        delivery_cost NUMERIC(14, 2),
        final_amount NUMERIC(14, 2),
        is_paid INT,
        is_cancelled INT,
        cancellation_reason TEXT,
        delivery_started_at TIMESTAMP,
        delivered_at TIMESTAMP,
        wait_minutes NUMERIC(14, 2),
        delivery_minutes NUMERIC(14, 2)
    );

    CREATE TABLE IF NOT EXISTS mart.mart_product_sales (
        order_date DATE NOT NULL,
        store_id BIGINT NOT NULL,
        item_id BIGINT NOT NULL,
        item_title TEXT,
        item_category TEXT,
        orders_count BIGINT,
        ordered_qty BIGINT,
        canceled_qty BIGINT,
        fulfilled_qty BIGINT,
        gross_revenue NUMERIC(14, 2),
        item_discount_sum NUMERIC(14, 2),
        replaced_count BIGINT
    );

    TRUNCATE TABLE mart.mart_order_delivery;
    TRUNCATE TABLE mart.mart_product_sales;
    """

    hook.run(sql)


def build_order_delivery_mart():
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    spark = _get_spark_session()
    try:
        orders_df = spark.read.format("jdbc").options(
            **JDBC_CONFIG,
            dbtable="dwh.orders",
        ).load()

        order_items_df = spark.read.format("jdbc").options(
            **JDBC_CONFIG,
            dbtable="dwh.order_items",
        ).load()

        assignments_raw_df = spark.read.format("jdbc").options(
            **JDBC_CONFIG,
            dbtable="dwh.order_driver_assignments",
        ).load()

        # агрегируем позиции заказа до уровня "1 строка = 1 заказ"
        items_agg_df = (
            order_items_df.groupBy("order_id")
            .agg(
                F.sum(F.coalesce(F.col("item_quantity"), F.lit(0))).cast("bigint").alias("items_count"),
                F.sum(
                    F.coalesce(F.col("item_quantity"), F.lit(0))
                    * F.coalesce(F.col("item_price"), F.lit(0))
                ).alias("gross_amount"),
                F.sum(F.coalesce(F.col("item_discount"), F.lit(0))).alias("item_discount_sum"),
            )
        )

        # оставляем одно назначение на заказ
        # берём последнее по delivery_started_at, а если оно NULL - оно уйдёт в конец
        w = Window.partitionBy("order_id").orderBy(F.col("delivery_started_at").desc_nulls_last())

        assignments_df = (
            assignments_raw_df
            .withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )

        mart_df = (
            orders_df.alias("o")
            .join(items_agg_df.alias("i"), on="order_id", how="left")
            .join(assignments_df.alias("a"), on="order_id", how="left")
            .withColumn("order_date", F.to_date("created_at"))
            .withColumn(
                "is_paid",
                F.when(F.col("paid_at").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            )
            .withColumn(
                "is_cancelled",
                F.when(F.col("canceled_at").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            )
            .withColumn(
                "wait_minutes",
                F.when(
                    F.col("delivery_started_at").isNotNull(),
                    (F.unix_timestamp("delivery_started_at") - F.unix_timestamp("created_at")) / 60.0
                )
            )
            .withColumn(
                "delivery_minutes",
                F.when(
                    F.col("delivery_started_at").isNotNull() & F.col("delivered_at").isNotNull(),
                    (F.unix_timestamp("delivered_at") - F.unix_timestamp("delivery_started_at")) / 60.0
                )
            )
            .withColumn(
                "gross_amount_num",
                F.coalesce(F.col("gross_amount").cast("double"), F.lit(0.0))
            )
            .withColumn(
                "item_discount_sum_num",
                F.coalesce(F.col("item_discount_sum").cast("double"), F.lit(0.0))
            )
            .withColumn(
                "order_discount_num",
                F.coalesce(F.col("order_discount").cast("double"), F.lit(0.0))
            )
            .withColumn(
                "delivery_cost_num",
                F.coalesce(F.col("delivery_cost").cast("double"), F.lit(0.0))
            )
            .withColumn(
                "final_amount",
                F.col("gross_amount_num")
                - F.col("item_discount_sum_num")
                - F.col("order_discount_num")
                + F.col("delivery_cost_num")
            )
            .select(
                "order_id",
                "order_date",
                "created_at",
                "user_id",
                "store_id",
                "driver_id",
                "payment_type",
                "items_count",
                F.round(F.col("gross_amount_num"), 2).alias("gross_amount"),
                F.round(F.col("item_discount_sum_num"), 2).alias("item_discount_sum"),
                F.round(F.col("order_discount_num"), 2).alias("order_discount"),
                F.round(F.col("delivery_cost_num"), 2).alias("delivery_cost"),
                F.round(F.col("final_amount"), 2).alias("final_amount"),
                "is_paid",
                "is_cancelled",
                F.col("order_cancellation_reason").alias("cancellation_reason"),
                "delivery_started_at",
                "delivered_at",
                F.round(F.col("wait_minutes"), 2).alias("wait_minutes"),
                F.round(F.col("delivery_minutes"), 2).alias("delivery_minutes"),
            )
        )

        mart_df.write.format("jdbc").options(
            **JDBC_CONFIG,
            dbtable="mart.mart_order_delivery",
        ).mode("append").save()

    finally:
        spark.stop()


def build_product_sales_mart():
    from pyspark.sql import functions as F

    spark = _get_spark_session()
    try:
        orders_df = spark.read.format("jdbc").options(
            **JDBC_CONFIG,
            dbtable="dwh.orders",
        ).load()

        order_items_df = spark.read.format("jdbc").options(
            **JDBC_CONFIG,
            dbtable="dwh.order_items",
        ).load()

        products_df = spark.read.format("jdbc").options(
            **JDBC_CONFIG,
            dbtable="dwh.products",
        ).load()

        mart_df = (
            order_items_df.alias("oi")
            .join(orders_df.alias("o"), on="order_id", how="inner")
            .join(products_df.alias("p"), on="item_id", how="left")
            .withColumn("order_date", F.to_date("created_at"))
            .withColumn("ordered_qty", F.coalesce(F.col("item_quantity"), F.lit(0)))
            .withColumn("canceled_qty", F.coalesce(F.col("item_canceled_quantity"), F.lit(0)))
            .withColumn(
                "fulfilled_qty",
                F.coalesce(F.col("item_quantity"), F.lit(0))
                - F.coalesce(F.col("item_canceled_quantity"), F.lit(0))
            )
            .groupBy(
                "order_date",
                "store_id",
                "item_id",
                "item_title",
                "item_category",
            )
            .agg(
                F.countDistinct("order_id").alias("orders_count"),
                F.sum("ordered_qty").cast("bigint").alias("ordered_qty"),
                F.sum("canceled_qty").cast("bigint").alias("canceled_qty"),
                F.sum("fulfilled_qty").cast("bigint").alias("fulfilled_qty"),
                F.sum(
                    F.coalesce(F.col("item_quantity"), F.lit(0))
                    * F.coalesce(F.col("item_price"), F.lit(0))
                ).alias("gross_revenue"),
                F.sum(F.coalesce(F.col("item_discount"), F.lit(0))).alias("item_discount_sum"),
                F.sum(
                    F.when(F.col("item_replaced_id").isNotNull(), F.lit(1)).otherwise(F.lit(0))
                ).cast("bigint").alias("replaced_count"),
            )
            .select(
                "order_date",
                "store_id",
                "item_id",
                "item_title",
                "item_category",
                "orders_count",
                "ordered_qty",
                "canceled_qty",
                "fulfilled_qty",
                F.round("gross_revenue", 2).alias("gross_revenue"),
                F.round("item_discount_sum", 2).alias("item_discount_sum"),
                "replaced_count",
            )
        )

        mart_df.write.format("jdbc").options(
            **JDBC_CONFIG,
            dbtable="mart.mart_product_sales",
        ).mode("append").save()

    finally:
        spark.stop()


with DAG(
    dag_id="build_marts_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    t_prepare = PythonOperator(
        task_id="prepare_mart_schema",
        python_callable=prepare_mart_schema,
    )

    t_order_delivery = PythonOperator(
        task_id="build_order_delivery_mart",
        python_callable=build_order_delivery_mart,
    )

    t_product_sales = PythonOperator(
        task_id="build_product_sales_mart",
        python_callable=build_product_sales_mart,
    )

    t_prepare >> [t_order_delivery, t_product_sales]