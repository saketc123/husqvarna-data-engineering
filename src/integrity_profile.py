from pyspark.sql import functions as F

from src.spark_session import get_spark_session


RAW_PATH = "data/raw"


def read_csv(spark, filename):
    reader = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
    )

    return reader.csv(f"{RAW_PATH}/{filename}")


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


def profile_orphan_reviews(reviews, orders):
    orphan_reviews = (
        reviews
        .select("review_id", "order_id", "review_score")
        .where(F.col("order_id").isNotNull())
        .join(
            orders.select("order_id").distinct(),
            on="order_id",
            how="left_anti",
        )
    )

    print("\nOrphan review records")
    print("=" * 70)

    print(f"Orphan review rows: {orphan_reviews.count()}")

    print("\nReview score distribution:")
    (
        orphan_reviews
        .groupBy("review_score")
        .count()
        .orderBy("review_score")
        .show(truncate=False)
    )

    print("\nSample orphan reviews:")
    orphan_reviews.show(20, truncate=False)

def main():
    spark = get_spark_session()

    orders = read_csv(spark, "orders.csv")
    customers = read_csv(spark, "customers.csv")
    order_items = read_csv(spark, "order_items.csv")
    products = read_csv(spark, "products.csv")
    sellers = read_csv(spark, "sellers.csv")
    payments = read_csv(spark, "order_payments.csv")
    reviews = read_csv(spark, "order_reviews.csv")

    print("\nRaw review schema")
    print("=" * 70)
    reviews.printSchema()

    print("\nReview column samples")
    print("=" * 70)
    reviews.show(10, truncate=False)

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

    print("\nReferential integrity results")
    print("=" * 70)

    for relationship, orphan_count in checks:
        print(f"{relationship}: {orphan_count} orphan values")

    profile_orphan_reviews(reviews, orders)

    spark.stop()


if __name__ == "__main__":
    main()