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


def print_null_counts(df, filename):
    print(f"\n{filename}")
    print("=" * 70)

    for column in df.columns:
        count = df.where(F.col(column).isNull()).count()
        if count > 0:
            print(f"{column}: {count}")


def main():
    spark = get_spark_session()

    orders = read_csv(spark, "orders.csv")
    products = read_csv(spark, "products.csv")
    reviews = read_csv(spark, "order_reviews.csv")

    print_null_counts(orders, "orders.csv")
    print_null_counts(products, "products.csv")
    print_null_counts(reviews, "order_reviews.csv")

    spark.stop()


if __name__ == "__main__":
    main()