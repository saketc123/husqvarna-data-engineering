from pyspark.sql import functions as F

from src.spark_session import get_spark_session


BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"
TAXONOMY_PATH = "taxonomy/category_translation.csv"


def read_bronze(spark, dataset_name):
    return spark.read.parquet(f"{BRONZE_PATH}/{dataset_name}")


def read_taxonomy(spark):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(TAXONOMY_PATH)
        .select(
            F.trim(F.col("source_category")).alias("source_category"),
            F.trim(F.col("normalized_category")).alias(
                "normalized_category"
            ),
            F.trim(F.col("category_family")).alias("category_family"),
        )
        .dropDuplicates(["source_category"])
    )


def build_customers(customers):
    return (
        customers
        .select(
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.trim(F.col("customer_unique_id")).alias("customer_unique_id"),
            F.col("customer_zip_code_prefix").alias(
                "customer_zip_code_prefix"
            ),
            F.upper(F.trim(F.col("customer_city"))).alias("customer_city"),
            F.upper(F.trim(F.col("customer_state"))).alias("customer_state"),
        )
        .dropDuplicates(["customer_id"])
    )


def build_orders(orders):
    return (
        orders
        .select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.lower(F.trim(F.col("order_status"))).alias("order_status"),
            F.col("purchased_at").alias("purchased_at"),
            F.col("approved_at").alias("approved_at"),
            F.col("carrier_handoff_at").alias("carrier_handoff_at"),
            F.col("delivered_at").alias("delivered_at"),
            F.col("est_delivery_date").alias("est_delivery_date"),
        )
        .dropDuplicates(["order_id"])
    )


def build_products(products, taxonomy):
    cleaned_products = (
        products
        .select(
            F.trim(F.col("product_id")).alias("product_id"),
            F.trim(F.col("product_category_name")).alias(
                "product_category_name"
            ),
            F.col("product_name_lenght").alias("product_name_length"),
            F.col("product_description_lenght").alias(
                "product_description_length"
            ),
            F.col("product_photos_qty").alias("product_photos_qty"),
            F.col("product_weight_g").alias("product_weight_g"),
            F.col("product_length_cm").alias("product_length_cm"),
            F.col("product_height_cm").alias("product_height_cm"),
            F.col("product_width_cm").alias("product_width_cm"),
        )
    )

    return (
        cleaned_products
        .join(
            taxonomy,
            cleaned_products.product_category_name
            == taxonomy.source_category,
            "left",
        )
        .select(
            cleaned_products.product_id,
            cleaned_products.product_category_name,
            taxonomy.normalized_category,
            taxonomy.category_family,
            cleaned_products.product_name_length,
            cleaned_products.product_description_length,
            cleaned_products.product_photos_qty,
            cleaned_products.product_weight_g,
            cleaned_products.product_length_cm,
            cleaned_products.product_height_cm,
            cleaned_products.product_width_cm,
        )
        .dropDuplicates(["product_id"])
    )


def build_sellers(sellers):
    return (
        sellers
        .select(
            F.trim(F.col("seller_id")).alias("seller_id"),
            F.col("seller_zip_code_prefix").alias(
                "seller_zip_code_prefix"
            ),
            F.upper(F.trim(F.col("seller_city"))).alias("seller_city"),
            F.upper(F.trim(F.col("seller_state"))).alias("seller_state"),
        )
        .dropDuplicates(["seller_id"])
    )


def build_order_items(order_items):
    return (
        order_items
        .select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.col("order_item_id").alias("order_item_id"),
            F.trim(F.col("product_id")).alias("product_id"),
            F.trim(F.col("seller_id")).alias("seller_id"),
            F.col("shipping_limit_date").alias("shipping_limit_date"),
            F.col("item_price").alias("item_price"),
            F.col("freight_cost").alias("freight_cost"),
        )
        .dropDuplicates(["order_id", "order_item_id"])
    )


def build_order_payments(payments):
    return (
        payments
        .select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.col("payment_sequential").alias("payment_sequential"),
            F.lower(F.trim(F.col("payment_type"))).alias("payment_type"),
            F.col("payment_installments").alias("payment_installments"),
            F.col("payment_value").alias("payment_value"),
        )
        .dropDuplicates(["order_id", "payment_sequential"])
    )


def build_order_reviews(reviews):
    return (
        reviews
        .select(
            F.trim(F.col("review_id")).alias("review_id"),
            F.trim(F.col("order_id")).alias("order_id"),
            F.col("review_score").cast("integer").alias("review_score"),
            F.trim(F.col("review_comment_title")).alias(
                "review_comment_title"
            ),
            F.trim(F.col("review_comment_message")).alias(
                "review_comment_message"
            ),
            F.col("review_creation_date").alias("review_creation_date"),
            F.col("review_answer_timestamp").alias(
                "review_answer_timestamp"
            ),
        )
    )


def build_geolocation(geolocation):
    return (
        geolocation
        .select(
            F.col("geolocation_zip_code_prefix").alias(
                "geolocation_zip_code_prefix"
            ),
            F.col("geolocation_lat").alias("geolocation_lat"),
            F.col("geolocation_lng").alias("geolocation_lng"),
            F.upper(F.trim(F.col("geolocation_city"))).alias(
                "geolocation_city"
            ),
            F.upper(F.trim(F.col("geolocation_state"))).alias(
                "geolocation_state"
            ),
        )
    )


def main():
    spark = get_spark_session()

    print("Silver transformation started")

    customers = read_bronze(spark, "customers")
    orders = read_bronze(spark, "orders")
    order_items = read_bronze(spark, "order_items")
    products = read_bronze(spark, "products")
    sellers = read_bronze(spark, "sellers")
    payments = read_bronze(spark, "order_payments")
    reviews = read_bronze(spark, "order_reviews")
    geolocation = read_bronze(spark, "geolocation")
    taxonomy = read_taxonomy(spark)

    print("Bronze datasets loaded successfully")
    print("Taxonomy loaded successfully")

    silver_customers = build_customers(customers)
    silver_customers.write.mode("overwrite").parquet(
        f"{SILVER_PATH}/customers"
    )
    print("Silver customers written successfully")

    silver_orders = build_orders(orders)
    silver_orders.write.mode("overwrite").parquet(
        f"{SILVER_PATH}/orders"
    )
    print("Silver orders written successfully")

    silver_products = build_products(products, taxonomy)
    silver_products.write.mode("overwrite").parquet(
        f"{SILVER_PATH}/products"
    )
    print("Silver products written successfully")

    silver_sellers = build_sellers(sellers)
    silver_sellers.write.mode("overwrite").parquet(
        f"{SILVER_PATH}/sellers"
    )
    print("Silver sellers written successfully")

    silver_order_items = build_order_items(order_items)
    silver_order_items.write.mode("overwrite").parquet(
        f"{SILVER_PATH}/order_items"
    )
    print("Silver order_items written successfully")

    silver_payments = build_order_payments(payments)
    silver_payments.write.mode("overwrite").parquet(
        f"{SILVER_PATH}/order_payments"
    )
    print("Silver order_payments written successfully")

    silver_reviews = build_order_reviews(reviews)
    silver_reviews.write.mode("overwrite").parquet(
        f"{SILVER_PATH}/order_reviews"
    )
    print("Silver order_reviews written successfully")

    silver_geolocation = build_geolocation(geolocation)
    silver_geolocation.write.mode("overwrite").parquet(
        f"{SILVER_PATH}/geolocation"
    )
    print("Silver geolocation written successfully")

    spark.stop()


if __name__ == "__main__":
    main()
