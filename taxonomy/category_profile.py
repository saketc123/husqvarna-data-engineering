from pyspark.sql import functions as F

from src.spark_session import get_spark_session


PRODUCTS_PATH = "data/raw/products.csv"


def main() -> None:
    spark = get_spark_session()

    products = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(PRODUCTS_PATH)
    )

    category_counts = (
        products
        .groupBy("product_category_name")
        .agg(F.count("*").alias("product_count"))
        .orderBy(
            F.col("product_count").desc(),
            F.col("product_category_name")
        )
    )

    print(f"Total product rows: {products.count()}")
    print(
        f"Products with missing category: "
        f"{products.filter(F.col('product_category_name').isNull()).count()}"
    )

    print("\nCategory frequencies:")
    category_counts.show(
        category_counts.count(),
        truncate=False
    )

    spark.stop()


if __name__ == "__main__":
    main()