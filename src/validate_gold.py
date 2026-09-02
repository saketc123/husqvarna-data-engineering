from pyspark.sql import functions as F

from src.spark_session import get_spark_session


GOLD_PATH = "data/gold"


def read_parquet(spark, name):
    return spark.read.parquet(f"{GOLD_PATH}/{name}")


def count_duplicate_groups(df, columns):
    return (
        df.groupBy(*columns)
        .count()
        .where(F.col("count") > 1)
        .count()
    )


def count_nulls(df, column):
    return df.where(F.col(column).isNull()).count()


def main():
    spark = get_spark_session()

    print("Gold validation started")
    print("=" * 70)

    sales = read_parquet(spark, "sales")
    delivery = read_parquet(spark, "delivery_performance")
    customers = read_parquet(spark, "customer_summary")
    products = read_parquet(spark, "product_performance")
    sellers = read_parquet(spark, "seller_performance")

    # ---------------------------------------------------------------
    # Row counts
    # ---------------------------------------------------------------

    print("\nRow counts")
    print("=" * 70)

    print(f"sales: {sales.count()}")
    print(f"delivery_performance: {delivery.count()}")
    print(f"customer_summary: {customers.count()}")
    print(f"product_performance: {products.count()}")
    print(f"seller_performance: {sellers.count()}")

    # ---------------------------------------------------------------
    # Grain / duplicate checks
    # ---------------------------------------------------------------

    print("\nDuplicate grain checks")
    print("=" * 70)

    checks = [
        (
            "sales.(order_id, order_item_id)",
            sales,
            ["order_id", "order_item_id"],
        ),
        (
            "delivery_performance.order_id",
            delivery,
            ["order_id"],
        ),
        (
            "customer_summary.customer_unique_id",
            customers,
            ["customer_unique_id"],
        ),
        (
            "product_performance.(product_id, product_category_name)",
            products,
            ["product_id", "product_category_name"],
        ),
        (
            "seller_performance.(seller_id, seller_city, seller_state)",
            sellers,
            ["seller_id", "seller_city", "seller_state"],
        ),
    ]

    for name, df, columns in checks:
        duplicates = count_duplicate_groups(df, columns)
        print(f"{name}: {duplicates} duplicate groups")

    # ---------------------------------------------------------------
    # Null business keys
    # ---------------------------------------------------------------

    print("\nNull business-key checks")
    print("=" * 70)

    print(
        f"sales.order_id nulls: "
        f"{count_nulls(sales, 'order_id')}"
    )

    print(
        f"sales.order_item_id nulls: "
        f"{count_nulls(sales, 'order_item_id')}"
    )

    print(
        f"sales.product_id nulls: "
        f"{count_nulls(sales, 'product_id')}"
    )

    print(
        f"sales.seller_id nulls: "
        f"{count_nulls(sales, 'seller_id')}"
    )

    print(
        f"delivery_performance.order_id nulls: "
        f"{count_nulls(delivery, 'order_id')}"
    )

    print(
        f"customer_summary.customer_unique_id nulls: "
        f"{count_nulls(customers, 'customer_unique_id')}"
    )

    print(
        f"product_performance.product_id nulls: "
        f"{count_nulls(products, 'product_id')}"
    )

    print(
        f"seller_performance.seller_id nulls: "
        f"{count_nulls(sellers, 'seller_id')}"
    )

    # ---------------------------------------------------------------
    # Delivery metrics
    # ---------------------------------------------------------------

    print("\nDelivery performance checks")
    print("=" * 70)

    delivered_orders = delivery.where(
        F.col("order_status") == "delivered"
    )

    print(
        f"Delivered orders: "
        f"{delivered_orders.count()}"
    )

    print(
        f"Delivered on time = true: "
        f"{delivery.where(F.col('delivered_on_time') == True).count()}"
    )

    print(
        f"Delivered on time = false: "
        f"{delivery.where(F.col('delivered_on_time') == False).count()}"
    )

    print(
        f"Delivered on time = null: "
        f"{delivery.where(F.col('delivered_on_time').isNull()).count()}"
    )

    print(
        f"Delivery delay > 0 days: "
        f"{delivery.where(F.col('delivery_delay_days') > 0).count()}"
    )

    print(
        f"Delivery delay <= 0 days: "
        f"{delivery.where(F.col('delivery_delay_days') <= 0).count()}"
    )

    # ---------------------------------------------------------------
    # Financial sanity checks
    # ---------------------------------------------------------------

    print("\nFinancial sanity checks")
    print("=" * 70)

    sales_metrics = sales.agg(
        F.sum("item_price").alias("total_item_revenue"),
        F.sum("freight_cost").alias("total_freight"),
        F.sum("total_payment_value").alias("total_payment_value"),
    ).collect()[0]

    print(
        f"Total item revenue: "
        f"{sales_metrics['total_item_revenue']}"
    )

    print(
        f"Total freight cost: "
        f"{sales_metrics['total_freight']}"
    )

    print(
        f"Sum of order payment totals across sales rows: "
        f"{sales_metrics['total_payment_value']}"
    )

    # ---------------------------------------------------------------
    # Summary samples
    # ---------------------------------------------------------------

    print("\nCustomer summary sample")
    print("=" * 70)
    customers.show(5, truncate=False)

    print("\nProduct performance sample")
    print("=" * 70)
    products.show(5, truncate=False)

    print("\nSeller performance sample")
    print("=" * 70)
    sellers.show(5, truncate=False)

    print("\nGold validation completed")

    spark.stop()


if __name__ == "__main__":
    main()