from pyspark.sql import functions as F

from src.spark_session import get_spark_session


SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold"


def read_parquet(spark, name):
    return spark.read.parquet(f"{SILVER_PATH}/{name}")


def add_surrogate_key(df, natural_key, surrogate_key):
    return df.withColumn(
        surrogate_key,
        F.xxhash64(F.col(natural_key)).cast("long"),
    )


def build_dim_customer(customers):
    return (
        customers
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        )
        .dropDuplicates(["customer_id"])
        .transform(
            lambda df: add_surrogate_key(
                df,
                "customer_id",
                "customer_key",
            )
        )
        .select(
            "customer_key",
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        )
    )


def build_dim_seller(sellers):
    return (
        sellers
        .select(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
        )
        .dropDuplicates(["seller_id"])
        .transform(
            lambda df: add_surrogate_key(
                df,
                "seller_id",
                "seller_key",
            )
        )
        .select(
            "seller_key",
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
        )
    )


def build_dim_product(products):
    return (
        products
        .select(
            "product_id",
            "product_category_name",
            "normalized_category",
            "category_family",
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        )
        .dropDuplicates(["product_id"])
        .transform(
            lambda df: add_surrogate_key(
                df,
                "product_id",
                "product_key",
            )
        )
        .select(
            "product_key",
            "product_id",
            "product_category_name",
            "normalized_category",
            "category_family",
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        )
    )


def build_dim_date(orders, reviews):
    order_dates = (
        orders
        .select(
            F.to_date("purchased_at").alias("date")
        )
        .where(F.col("date").isNotNull())
    )

    review_dates = (
        reviews
        .select(
            F.to_date("review_creation_date").alias("date")
        )
        .where(F.col("date").isNotNull())
    )

    dates = (
        order_dates
        .union(review_dates)
        .distinct()
    )

    return (
        dates
        .withColumn(
            "date_key",
            F.date_format("date", "yyyyMMdd").cast("int"),
        )
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("month", F.month("date"))
        .withColumn(
            "month_name",
            F.date_format("date", "MMMM"),
        )
        .withColumn(
            "week_of_year",
            F.weekofyear("date"),
        )
        .withColumn(
            "day_of_month",
            F.dayofmonth("date"),
        )
        .withColumn(
            "day_of_week",
            F.dayofweek("date"),
        )
        .withColumn(
            "day_name",
            F.date_format("date", "EEEE"),
        )
        .select(
            "date_key",
            "date",
            "year",
            "quarter",
            "month",
            "month_name",
            "week_of_year",
            "day_of_month",
            "day_of_week",
            "day_name",
        )
    )


def build_fact_order_item(
    orders,
    order_items,
    dim_customer,
    dim_seller,
    dim_product,
):
    return (
        order_items
        .join(
            orders.select(
                "order_id",
                "customer_id",
                "order_status",
                "purchased_at",
                "approved_at",
                "carrier_handoff_at",
                "delivered_at",
                "est_delivery_date",
            ),
            on="order_id",
            how="left",
        )
        .join(
            dim_customer.select(
                "customer_key",
                "customer_id",
            ),
            on="customer_id",
            how="left",
        )
        .join(
            dim_seller.select(
                "seller_key",
                "seller_id",
                "seller_state",
            ),
            on="seller_id",
            how="left",
        )
        .join(
            dim_product.select(
                "product_key",
                "product_id",
            ),
            on="product_id",
            how="left",
        )
        .withColumn(
            "purchase_date_key",
            F.date_format(
                F.to_date("purchased_at"),
                "yyyyMMdd",
            ).cast("int"),
        )
        .withColumn(
            "purchase_year",
            F.year("purchased_at"),
        )
        .select(
            "order_id",
            "order_item_id",
            "customer_key",
            "seller_key",
            "product_key",
            "purchase_date_key",
            "purchase_year",
            "seller_state",
            "order_status",
            "purchased_at",
            "approved_at",
            "carrier_handoff_at",
            "delivered_at",
            "est_delivery_date",
            "shipping_limit_date",
            "item_price",
            "freight_cost",
        )
    )


def build_fact_payment(
    payments,
    orders,
    dim_customer,
):
    return (
        payments
        .join(
            orders.select(
                "order_id",
                "customer_id",
                "purchased_at",
            ),
            on="order_id",
            how="left",
        )
        .join(
            dim_customer.select(
                "customer_key",
                "customer_id",
            ),
            on="customer_id",
            how="left",
        )
        .withColumn(
            "payment_date_key",
            F.date_format(
                F.to_date("purchased_at"),
                "yyyyMMdd",
            ).cast("int"),
        )
        .withColumn(
            "payment_year",
            F.year("purchased_at"),
        )
        .select(
            "order_id",
            "payment_sequential",
            "customer_key",
            "payment_date_key",
            "payment_year",
            "payment_type",
            "payment_installments",
            "payment_value",
        )
    )


def build_fact_review(
    reviews,
    orders,
    order_items,
    dim_customer,
    dim_product,
):
    return (
        reviews
        .join(
            orders.select(
                "order_id",
                "customer_id",
                "purchased_at",
            ),
            on="order_id",
            how="left",
        )
        .join(
            dim_customer.select(
                "customer_key",
                "customer_id",
            ),
            on="customer_id",
            how="left",
        )
        .join(
            order_items.select(
                "order_id",
                "product_id",
            ).dropDuplicates(
                ["order_id", "product_id"]
            ),
            on="order_id",
            how="left",
        )
        .join(
            dim_product.select(
                "product_key",
                "product_id",
                "normalized_category",
                "category_family",
            ),
            on="product_id",
            how="left",
        )
        .withColumn(
            "review_date_key",
            F.date_format(
                F.to_date("review_creation_date"),
                "yyyyMMdd",
            ).cast("int"),
        )
        .withColumn(
            "review_year",
            F.year("review_creation_date"),
        )
        .select(
            "review_id",
            "order_id",
            "customer_key",
            "product_key",
            "review_date_key",
            "review_year",
            "review_score",
            "review_creation_date",
            "review_answer_timestamp",
            "normalized_category",
            "category_family",
        )
        .dropDuplicates(
            ["review_id", "order_id", "product_key"]
        )
    )


def validate_fact_grain(fact, grain_columns, fact_name):
    duplicates = (
        fact
        .groupBy(*grain_columns)
        .count()
        .where(F.col("count") > 1)
        .count()
    )

    if duplicates != 0:
        raise RuntimeError(
            f"Duplicate grain in {fact_name}: "
            f"{duplicates} duplicate groups"
        )


def validate_referential_integrity(
    fact,
    fact_key,
    dimension,
    dimension_key,
):
    fact_alias = fact.alias("fact")
    dimension_alias = dimension.select(
        F.col(dimension_key).alias("dim_key")
    ).alias("dimension")

    orphan_count = (
        fact_alias
        .select(F.col(f"fact.{fact_key}").alias("fact_key"))
        .where(F.col("fact_key").isNotNull())
        .join(
            dimension_alias,
            F.col("fact_key") == F.col("dimension.dim_key"),
            "left_anti",
        )
        .count()
    )

    if orphan_count != 0:
        raise RuntimeError(
            f"Referential integrity failure: "
            f"{fact_key} -> {dimension_key}; "
            f"{orphan_count} orphan keys"
        )


def write_partitioned(df, path, partition_columns):
    (
        df.write
        .mode("overwrite")
        .partitionBy(*partition_columns)
        .parquet(path)
    )


def main():
    spark = get_spark_session()

    print("Gold star-schema transformation started")

    orders = read_parquet(spark, "orders")
    customers = read_parquet(spark, "customers")
    products = read_parquet(spark, "products")
    sellers = read_parquet(spark, "sellers")
    order_items = read_parquet(spark, "order_items")
    payments = read_parquet(spark, "order_payments")
    reviews = read_parquet(spark, "order_reviews")

    print("All Silver datasets loaded successfully")

    dim_customer = build_dim_customer(customers)
    dim_seller = build_dim_seller(sellers)
    dim_product = build_dim_product(products)
    dim_date = build_dim_date(orders, reviews)

    fact_order_item = build_fact_order_item(
        orders,
        order_items,
        dim_customer,
        dim_seller,
        dim_product,
    )

    fact_payment = build_fact_payment(
        payments,
        orders,
        dim_customer,
    )

    fact_review = build_fact_review(
        reviews,
        orders,
        order_items,
        dim_customer,
        dim_product,
    )

    print("Gold dimensions and facts built")

    validate_fact_grain(
        fact_order_item,
        ["order_id", "order_item_id"],
        "fact_order_item",
    )

    validate_fact_grain(
        fact_payment,
        ["order_id", "payment_sequential"],
        "fact_payment",
    )

    validate_fact_grain(
        fact_review,
        ["review_id", "order_id", "product_key"],
        "fact_review",
    )

    validate_referential_integrity(
        fact_order_item,
        "customer_key",
        dim_customer,
        "customer_key",
    )

    validate_referential_integrity(
        fact_order_item,
        "seller_key",
        dim_seller,
        "seller_key",
    )

    validate_referential_integrity(
        fact_order_item,
        "product_key",
        dim_product,
        "product_key",
    )

    validate_referential_integrity(
        fact_order_item,
        "purchase_date_key",
        dim_date,
        "date_key",
    )

    validate_referential_integrity(
        fact_payment,
        "customer_key",
        dim_customer,
        "customer_key",
    )

    validate_referential_integrity(
        fact_payment,
        "payment_date_key",
        dim_date,
        "date_key",
    )

    validate_referential_integrity(
        fact_review,
        "customer_key",
        dim_customer,
        "customer_key",
    )

    validate_referential_integrity(
        fact_review,
        "product_key",
        dim_product,
        "product_key",
    )

    validate_referential_integrity(
        fact_review,
        "review_date_key",
        dim_date,
        "date_key",
    )

    print("Gold referential integrity checks passed")

    dim_customer.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/dim_customer"
    )

    dim_seller.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/dim_seller"
    )

    dim_product.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/dim_product"
    )

    dim_date.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/dim_date"
    )

    write_partitioned(
        fact_order_item,
        f"{GOLD_PATH}/fact_order_item",
        ["purchase_year", "seller_state"],
    )

    write_partitioned(
        fact_payment,
        f"{GOLD_PATH}/fact_payment",
        ["payment_year"],
    )

    write_partitioned(
        fact_review,
        f"{GOLD_PATH}/fact_review",
        ["review_year"],
    )

    print("Gold star schema written successfully")

    print(f"dim_customer: {dim_customer.count()}")
    print(f"dim_seller: {dim_seller.count()}")
    print(f"dim_product: {dim_product.count()}")
    print(f"dim_date: {dim_date.count()}")
    print(f"fact_order_item: {fact_order_item.count()}")
    print(f"fact_payment: {fact_payment.count()}")
    print(f"fact_review: {fact_review.count()}")

    spark.stop()


if __name__ == "__main__":
    main()