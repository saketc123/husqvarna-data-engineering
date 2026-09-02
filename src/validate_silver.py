from pyspark.sql import functions as F

from src.spark_session import get_spark_session


SILVER_PATH = "data/silver"


def read_parquet(spark, name):
    return spark.read.parquet(f"{SILVER_PATH}/{name}")


def count_duplicate_groups(df, columns):
    return (
        df.groupBy(*columns)
        .count()
        .where(F.col("count") > 1)
        .count()
    )


def count_orphans(child_df, child_column, parent_df, parent_column):
    child_keys = (
        child_df
        .select(F.col(child_column).alias("child_key"))
        .where(F.col("child_key").isNotNull())
        .distinct()
    )

    parent_keys = (
        parent_df
        .select(F.col(parent_column).alias("parent_key"))
        .where(F.col("parent_key").isNotNull())
        .distinct()
    )

    return (
        child_keys
        .join(
            parent_keys,
            F.col("child_key") == F.col("parent_key"),
            "left_anti",
        )
        .count()
    )


def main():
    spark = get_spark_session()

    print("Silver validation started")
    print("=" * 70)

    customers = read_parquet(spark, "customers")
    orders = read_parquet(spark, "orders")
    products = read_parquet(spark, "products")
    sellers = read_parquet(spark, "sellers")
    order_items = read_parquet(spark, "order_items")
    payments = read_parquet(spark, "order_payments")
    reviews = read_parquet(spark, "order_reviews")
    geolocation = read_parquet(spark, "geolocation")

    datasets = {
        "customers": customers,
        "orders": orders,
        "products": products,
        "sellers": sellers,
        "order_items": order_items,
        "order_payments": payments,
        "order_reviews": reviews,
        "geolocation": geolocation,
    }

    print("\nRow counts")
    print("=" * 70)

    for name, df in datasets.items():
        print(f"{name}: {df.count()}")

    print("\nDuplicate key results")
    print("=" * 70)

    duplicate_checks = [
        ("customers.customer_id", customers, ["customer_id"]),
        ("products.product_id", products, ["product_id"]),
        ("sellers.seller_id", sellers, ["seller_id"]),
        ("orders.order_id", orders, ["order_id"]),
        (
            "order_items.(order_id, order_item_id)",
            order_items,
            ["order_id", "order_item_id"],
        ),
        (
            "order_payments.(order_id, payment_sequential)",
            payments,
            ["order_id", "payment_sequential"],
        ),
    ]

    for name, df, columns in duplicate_checks:
        duplicates = count_duplicate_groups(df, columns)
        print(f"{name}: {duplicates} duplicate key groups")

    print("\nReferential integrity results")
    print("=" * 70)

    checks = [
        (
            "orders.customer_id -> customers.customer_id",
            count_orphans(
                orders,
                "customer_id",
                customers,
                "customer_id",
            ),
        ),
        (
            "order_items.order_id -> orders.order_id",
            count_orphans(
                order_items,
                "order_id",
                orders,
                "order_id",
            ),
        ),
        (
            "order_items.product_id -> products.product_id",
            count_orphans(
                order_items,
                "product_id",
                products,
                "product_id",
            ),
        ),
        (
            "order_items.seller_id -> sellers.seller_id",
            count_orphans(
                order_items,
                "seller_id",
                sellers,
                "seller_id",
            ),
        ),
        (
            "order_payments.order_id -> orders.order_id",
            count_orphans(
                payments,
                "order_id",
                orders,
                "order_id",
            ),
        ),
        (
            "order_reviews.order_id -> orders.order_id",
            count_orphans(
                reviews,
                "order_id",
                orders,
                "order_id",
            ),
        ),
    ]

    for relationship, orphan_count in checks:
        print(f"{relationship}: {orphan_count} orphan values")

    print("\nSilver validation completed")

    spark.stop()


if __name__ == "__main__":
    main()