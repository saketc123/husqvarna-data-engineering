from datetime import datetime, timezone

from pyspark.sql import functions as F

from src.spark_session import get_spark_session


RAW_PATH = "data/raw"
BRONZE_PATH = "data/bronze"


RAW_DATASETS = [
    "customers",
    "geolocation",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
]


def read_raw_csv(spark, dataset_name):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(f"{RAW_PATH}/{dataset_name}.csv")
    )


def build_bronze(spark, dataset_name):
    df = read_raw_csv(spark, dataset_name)

    ingestion_ts = datetime.now(timezone.utc)

    return df.withColumn(
        "ingestion_ts",
        F.lit(ingestion_ts).cast("timestamp"),
    )


def main():
    spark = get_spark_session()

    print("Bronze ingestion started")

    for dataset_name in RAW_DATASETS:
        bronze_df = build_bronze(spark, dataset_name)

        output_path = f"{BRONZE_PATH}/{dataset_name}"

        bronze_df.write.mode("append").parquet(output_path)

        print(
            f"Bronze {dataset_name} written successfully"
        )

    print("Bronze ingestion completed")

    spark.stop()


if __name__ == "__main__":
    main()