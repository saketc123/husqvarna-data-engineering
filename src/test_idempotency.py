import hashlib
import subprocess
import sys

from pyspark.sql import functions as F

from src.spark_session import get_spark_session


GOLD_PATH = "data/gold"

GOLD_TABLES = [
    "dim_customer",
    "dim_seller",
    "dim_product",
    "dim_date",
    "fact_order_item",
    "fact_payment",
    "fact_review",
]


def logical_hash(spark, table_name):
    df = spark.read.parquet(
        f"{GOLD_PATH}/{table_name}"
    )

    columns = sorted(df.columns)

    row_hashes = (
        df
        .select(
            F.sha2(
                F.to_json(
                    F.struct(
                        *[
                            F.col(column).alias(column)
                            for column in columns
                        ]
                    )
                ),
                256,
            ).alias("row_hash")
        )
        .orderBy("row_hash")
        .collect()
    )

    hasher = hashlib.sha256()

    for row in row_hashes:
        hasher.update(
            row["row_hash"].encode("utf-8")
        )

    return hasher.hexdigest()


def calculate_hashes(spark):
    return {
        table: logical_hash(spark, table)
        for table in GOLD_TABLES
    }


def run_gold():
    result = subprocess.run(
        [sys.executable, "-m", "src.build_gold"],
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "Gold transformation failed during idempotency test."
        )


def main():
    print("=" * 70)
    print("GOLD IDEMPOTENCY TEST")
    print("=" * 70)

    spark = get_spark_session()

    print("Capturing first logical Gold hashes...")

    first_hashes = calculate_hashes(spark)

    print("\nFirst-run hashes:")
    for table, value in first_hashes.items():
        print(f"{table}: {value}")

    spark.stop()

    print("\nRe-running Gold transformation...")
    run_gold()

    spark = get_spark_session()

    print("\nCapturing second logical Gold hashes...")

    second_hashes = calculate_hashes(spark)

    print("\nSecond-run hashes:")
    for table, value in second_hashes.items():
        print(f"{table}: {value}")

    print("\n" + "=" * 70)
    print("HASH COMPARISON")
    print("=" * 70)

    all_match = True

    for table in GOLD_TABLES:
        if first_hashes[table] == second_hashes[table]:
            print(f"PASS: {table} logical output is identical")
        else:
            print(f"FAIL: {table} logical output changed")
            all_match = False

    spark.stop()

    print("=" * 70)

    if not all_match:
        raise RuntimeError(
            "Gold logical idempotency test failed."
        )

    print("Gold idempotency test passed")


if __name__ == "__main__":
    main()