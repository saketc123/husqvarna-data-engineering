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


def duplicate_count(df, columns):
    return (
        df.groupBy(*columns)
        .count()
        .where(F.col("count") > 1)
        .count()
    )


def profile_duplicate_reviews(reviews):
    duplicates = (
        reviews
        .groupBy("review_id")
        .count()
        .where(F.col("count") > 1)
        .orderBy(F.col("count").desc())
    )

    print("\nDuplicate review IDs")
    print("=" * 70)
    print(f"Duplicate review_id groups: {duplicates.count()}")

    duplicate_rows = (
        reviews.alias("r")
        .join(
            duplicates.select("review_id").alias("d"),
            on="review_id",
            how="inner",
        )
        .orderBy("review_id", "order_id")
    )

    duplicate_rows.show(30, truncate=False)

    print("\nNumber of orders per duplicate review_id")
    print("=" * 70)

    (
        duplicate_rows
        .groupBy("review_id")
        .agg(F.countDistinct("order_id").alias("order_count"))
        .groupBy("order_count")
        .count()
        .orderBy("order_count")
        .show(truncate=False)
    )


def main():
    spark = get_spark_session()

    customers = read_csv(spark, "customers.csv")
    products = read_csv(spark, "products.csv")
    sellers = read_csv(spark, "sellers.csv")
    orders = read_csv(spark, "orders.csv")
    order_items = read_csv(spark, "order_items.csv")
    payments = read_csv(spark, "order_payments.csv")
    reviews = read_csv(spark, "order_reviews.csv")

    checks = [
        ("customers.customer_id", customers, ["customer_id"]),
        ("products.product_id", products, ["product_id"]),
        ("sellers.seller_id", sellers, ["seller_id"]),
        ("orders.order_id", orders, ["order_id"]),
        ("order_reviews.review_id", reviews, ["review_id"]),
        (
            "order_reviews.(order_id, review_id)",
            reviews,
            ["order_id", "review_id"],
        ),
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

    print("\nDuplicate key results")
    print("=" * 70)

    for name, df, columns in checks:
        duplicate_groups = duplicate_count(df, columns)
        print(f"{name}: {duplicate_groups} duplicate key groups")

    profile_duplicate_reviews(reviews)

    print("\nReviews with missing review_id")
    print("=" * 70)

    (
        reviews
        .where(F.col("review_id").isNull())
        .show(truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()