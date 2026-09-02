from pathlib import Path

from spark_session import get_spark_session


RAW_DATA_PATH = Path("data/raw")


def profile_file(spark, file_path: Path) -> None:
    """Print basic profiling information for one CSV file."""

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(file_path))
    )

    print("\n" + "=" * 80)
    print(f"FILE: {file_path.name}")
    print("=" * 80)

    print(f"Rows: {df.count()}")
    print(f"Columns: {len(df.columns)}")

    print("\nSchema:")
    df.printSchema()

    print("\nColumns:")
    print(df.columns)

    print("\nNull counts:")
    for column in df.columns:
        null_count = df.filter(df[column].isNull()).count()
        print(f"  {column}: {null_count}")


def main() -> None:
    spark = get_spark_session()

    csv_files = sorted(RAW_DATA_PATH.glob("*.csv"))

    print(f"Found {len(csv_files)} CSV files.")

    for file_path in csv_files:
        profile_file(spark, file_path)

    spark.stop()


if __name__ == "__main__":
    main()