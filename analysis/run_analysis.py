from pathlib import Path

from src.spark_session import get_spark_session


GOLD_PATH = "data/gold"
SQL_PATH = Path("analysis/analysis.sql")


GOLD_TABLES = [
    "dim_customer",
    "dim_seller",
    "dim_product",
    "dim_date",
    "fact_order_item",
    "fact_payment",
    "fact_review",
]


def main():
    spark = get_spark_session()

    print("=" * 70)
    print("HUSQVARNA ANALYTICAL LAYER")
    print("=" * 70)

    for table in GOLD_TABLES:
        df = spark.read.parquet(
            f"{GOLD_PATH}/{table}"
        )

        df.createOrReplaceTempView(table)

        print(
            f"Registered Spark SQL view: {table}"
        )

    sql_text = SQL_PATH.read_text(
        encoding="utf-8"
    )

    statements = [
        statement.strip()
        for statement in sql_text.split(";")
        if statement.strip()
    ]

    for index, statement in enumerate(
        statements,
        start=1,
    ):
        print("\n" + "=" * 70)
        print(f"ANALYSIS QUERY {index}")
        print("=" * 70)

        result = spark.sql(statement)

        result.show(
            20,
            truncate=False,
        )

    spark.stop()


if __name__ == "__main__":
    main()