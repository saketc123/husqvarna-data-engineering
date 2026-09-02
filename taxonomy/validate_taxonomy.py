from pyspark.sql import functions as F

from src.spark_session import get_spark_session


PRODUCTS_PATH = "data/raw/products.csv"
TAXONOMY_PATH = "taxonomy/category_translation.csv"


def main() -> None:
    spark = get_spark_session()

    products = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(PRODUCTS_PATH)
    )

    taxonomy = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(TAXONOMY_PATH)
    )

    product_categories = (
        products
        .select("product_category_name")
        .where(F.col("product_category_name").isNotNull())
        .distinct()
    )

    taxonomy_categories = (
        taxonomy
        .select("source_category")
        .where(F.col("source_category").isNotNull())
        .distinct()
    )

    missing_from_taxonomy = (
        product_categories
        .join(
            taxonomy_categories,
            product_categories.product_category_name
            == taxonomy_categories.source_category,
            "left_anti",
        )
        .orderBy("product_category_name")
    )

    extra_in_taxonomy = (
        taxonomy_categories
        .join(
            product_categories,
            taxonomy_categories.source_category
            == product_categories.product_category_name,
            "left_anti",
        )
        .orderBy("source_category")
    )

    duplicate_source_categories = (
        taxonomy
        .groupBy("source_category")
        .count()
        .where(F.col("count") > 1)
        .orderBy("source_category")
    )

    print(f"Product categories: {product_categories.count()}")
    print(f"Taxonomy categories: {taxonomy_categories.count()}")

    print("\nCategories missing from taxonomy:")
    missing_from_taxonomy.show(truncate=False)

    print("\nExtra categories in taxonomy:")
    extra_in_taxonomy.show(truncate=False)

    print("\nDuplicate source categories in taxonomy:")
    duplicate_source_categories.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()