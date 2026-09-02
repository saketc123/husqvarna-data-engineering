from pyspark.sql import functions as F

from src.spark_session import get_spark_session


GOLD_PATH = "data/gold"
BRONZE_PATH = "data/bronze"

# Assumption: a successful pipeline run must have produced
# a Bronze ingestion batch within the last 24 hours.
FRESHNESS_HOURS = 24


def read_gold(spark, name):
    return spark.read.parquet(f"{GOLD_PATH}/{name}")


def read_bronze(spark, name):
    return spark.read.parquet(f"{BRONZE_PATH}/{name}")


def run_test(test_name, rows_evaluated, rows_failed):
    if rows_failed == 0:
        status = "PASS"
    else:
        status = "FAIL"

    if rows_evaluated is None:
        failure_pct = None
        failure_pct_display = "N/A"
        evaluated_display = "N/A"
    else:
        failure_pct = (
            (rows_failed / rows_evaluated) * 100
            if rows_evaluated > 0
            else 0.0
        )
        failure_pct_display = f"{failure_pct:.2f}%"
        evaluated_display = f"{rows_evaluated:,}"

    print(
        f"{status}: {test_name} | "
        f"evaluated={evaluated_display} | "
        f"failed={rows_failed:,} | "
        f"failure_rate={failure_pct_display}"
    )

    return {
        "test_name": test_name,
        "rows_evaluated": rows_evaluated,
        "rows_failed": rows_failed,
        "failure_pct": failure_pct,
        "passed": rows_failed == 0,
    }


def main():
    spark = get_spark_session()

    print("=" * 100)
    print("DATA QUALITY TESTS")
    print("=" * 100)

    dim_customer = read_gold(spark, "dim_customer")
    dim_seller = read_gold(spark, "dim_seller")
    dim_product = read_gold(spark, "dim_product")
    dim_date = read_gold(spark, "dim_date")
    fact_order_item = read_gold(spark, "fact_order_item")
    fact_payment = read_gold(spark, "fact_payment")
    fact_review = read_gold(spark, "fact_review")

    results = []

    # ------------------------------------------------------------------
    # 1. Customer surrogate key integrity
    # Evaluated = all dimension rows.
    # Failed = rows with a null surrogate key.
    # ------------------------------------------------------------------
    dim_customer_evaluated = dim_customer.count()
    dim_customer_failed = (
        dim_customer
        .filter(F.col("customer_key").isNull())
        .count()
    )

    results.append(
        run_test(
            "dim_customer.customer_key is non-null",
            dim_customer_evaluated,
            dim_customer_failed,
        )
    )

    # ------------------------------------------------------------------
    # 2. Seller surrogate key integrity
    # ------------------------------------------------------------------
    dim_seller_evaluated = dim_seller.count()
    dim_seller_failed = (
        dim_seller
        .filter(F.col("seller_key").isNull())
        .count()
    )

    results.append(
        run_test(
            "dim_seller.seller_key is non-null",
            dim_seller_evaluated,
            dim_seller_failed,
        )
    )

    # ------------------------------------------------------------------
    # 3. Product surrogate key integrity
    # ------------------------------------------------------------------
    dim_product_evaluated = dim_product.count()
    dim_product_failed = (
        dim_product
        .filter(F.col("product_key").isNull())
        .count()
    )

    results.append(
        run_test(
            "dim_product.product_key is non-null",
            dim_product_evaluated,
            dim_product_failed,
        )
    )

    # ------------------------------------------------------------------
    # 4. Fact -> customer RI
    # Evaluated = non-null customer foreign keys in the fact.
    # Failed = orphan foreign keys.
    # ------------------------------------------------------------------
    customer_keys = dim_customer.select(
        "customer_key"
    ).distinct()

    customer_fk_rows = (
        fact_order_item
        .filter(F.col("customer_key").isNotNull())
    )

    customer_fk_evaluated = customer_fk_rows.count()

    customer_orphans = (
        customer_fk_rows
        .select("customer_key")
        .join(
            customer_keys,
            on="customer_key",
            how="left_anti",
        )
        .count()
    )

    results.append(
        run_test(
            "fact_order_item.customer_key has no orphans",
            customer_fk_evaluated,
            customer_orphans,
        )
    )

    # ------------------------------------------------------------------
    # 5. Fact -> product RI
    # ------------------------------------------------------------------
    product_keys = dim_product.select(
        "product_key"
    ).distinct()

    product_fk_rows = (
        fact_order_item
        .filter(F.col("product_key").isNotNull())
    )

    product_fk_evaluated = product_fk_rows.count()

    product_orphans = (
        product_fk_rows
        .select("product_key")
        .join(
            product_keys,
            on="product_key",
            how="left_anti",
        )
        .count()
    )

    results.append(
        run_test(
            "fact_order_item.product_key has no orphans",
            product_fk_evaluated,
            product_orphans,
        )
    )

    # ------------------------------------------------------------------
    # 6. Fact grain: one row per order item
    # Evaluated = number of distinct grain keys.
    # Failed = number of grain groups appearing more than once.
    # ------------------------------------------------------------------
    order_item_grain = (
        fact_order_item
        .groupBy("order_id", "order_item_id")
        .count()
    )

    order_item_evaluated = order_item_grain.count()

    duplicate_order_items = (
        order_item_grain
        .filter(F.col("count") > 1)
        .count()
    )

    results.append(
        run_test(
            "fact_order_item has unique "
            "(order_id, order_item_id) grain",
            order_item_evaluated,
            duplicate_order_items,
        )
    )

    # ------------------------------------------------------------------
    # 7. Payment fact grain
    # ------------------------------------------------------------------
    payment_grain = (
        fact_payment
        .groupBy("order_id", "payment_sequential")
        .count()
    )

    payment_evaluated = payment_grain.count()

    duplicate_payments = (
        payment_grain
        .filter(F.col("count") > 1)
        .count()
    )

    results.append(
        run_test(
            "fact_payment has unique "
            "(order_id, payment_sequential) grain",
            payment_evaluated,
            duplicate_payments,
        )
    )

    # ------------------------------------------------------------------
    # 8. Temporal sanity
    # Evaluated = rows where both timestamps are available.
    # Failed = delivered_at earlier than purchased_at.
    # ------------------------------------------------------------------
    temporal_rows = (
        fact_order_item
        .filter(
            F.col("delivered_at").isNotNull()
            & F.col("purchased_at").isNotNull()
        )
    )

    temporal_evaluated = temporal_rows.count()

    invalid_delivery_dates = (
        temporal_rows
        .filter(
            F.col("delivered_at")
            < F.col("purchased_at")
        )
        .count()
    )

    results.append(
        run_test(
            "delivered_at is not before purchased_at",
            temporal_evaluated,
            invalid_delivery_dates,
        )
    )

    # ------------------------------------------------------------------
    # 9. Payment value sanity
    # Evaluated = non-null payment values.
    # Failed = negative payment values.
    # ------------------------------------------------------------------
    payment_value_rows = (
        fact_payment
        .filter(F.col("payment_value").isNotNull())
    )

    payment_value_evaluated = payment_value_rows.count()

    negative_payments = (
        payment_value_rows
        .filter(F.col("payment_value") < 0)
        .count()
    )

    results.append(
        run_test(
            "payment_value is non-negative",
            payment_value_evaluated,
            negative_payments,
        )
    )

    # ------------------------------------------------------------------
    # 10. Product taxonomy completeness
    # Evaluated = products with a non-null source category.
    # Failed = products whose category is not fully mapped.
    # ------------------------------------------------------------------
    taxonomy_rows = (
        dim_product
        .filter(F.col("product_category_name").isNotNull())
    )

    taxonomy_evaluated = taxonomy_rows.count()

    unmapped_categories = (
        taxonomy_rows
        .filter(
            F.col("normalized_category").isNull()
            | F.col("category_family").isNull()
        )
        .count()
    )

    results.append(
        run_test(
            "all non-null product categories are "
            "mapped to taxonomy",
            taxonomy_evaluated,
            unmapped_categories,
        )
    )

    # ------------------------------------------------------------------
    # 11. Review fact grain
    # Evaluated = distinct review grain groups.
    # Failed = grain groups appearing more than once.
    # ------------------------------------------------------------------
    review_grain = (
        fact_review
        .groupBy(
            "review_id",
            "order_id",
            "product_key",
        )
        .count()
    )

    review_grain_evaluated = review_grain.count()

    duplicate_reviews = (
        review_grain
        .filter(F.col("count") > 1)
        .count()
    )

    results.append(
        run_test(
            "fact_review has unique "
            "(review_id, order_id, product_key) grain",
            review_grain_evaluated,
            duplicate_reviews,
        )
    )

    # ------------------------------------------------------------------
    # 12. Review score sanity
    # Evaluated = all review rows.
    # Failed = null or out-of-range scores.
    # ------------------------------------------------------------------
    review_score_evaluated = fact_review.count()

    invalid_review_scores = (
        fact_review
        .filter(
            F.col("review_score").isNull()
            | ~F.col("review_score").between(1, 5)
        )
        .count()
    )

    results.append(
        run_test(
            "fact_review scores are between 1 and 5",
            review_score_evaluated,
            invalid_review_scores,
        )
    )

    # ------------------------------------------------------------------
    # 13. Review date key integrity
    # Evaluated = non-null review date keys.
    # Failed = orphan date keys.
    # ------------------------------------------------------------------
    review_date_rows = (
        fact_review
        .select("review_date_key")
        .where(F.col("review_date_key").isNotNull())
    )

    review_date_evaluated = review_date_rows.count()

    invalid_review_dates = (
        review_date_rows
        .join(
            dim_date.select(
                F.col("date_key").alias("dim_date_key")
            ),
            F.col("review_date_key")
            == F.col("dim_date_key"),
            how="left_anti",
        )
        .count()
    )

    results.append(
        run_test(
            "fact_review.review_date_key has no orphans",
            review_date_evaluated,
            invalid_review_dates,
        )
    )

    # ------------------------------------------------------------------
    # 14. Review customer key integrity
    # ------------------------------------------------------------------
    review_customer_rows = (
        fact_review
        .select("customer_key")
        .where(F.col("customer_key").isNotNull())
    )

    review_customer_evaluated = review_customer_rows.count()

    review_customer_orphans = (
        review_customer_rows
        .join(
            customer_keys,
            on="customer_key",
            how="left_anti",
        )
        .count()
    )

    results.append(
        run_test(
            "fact_review.customer_key has no orphans",
            review_customer_evaluated,
            review_customer_orphans,
        )
    )

    # ------------------------------------------------------------------
    # 15. Review product key integrity
    # Important: product_key is legitimately nullable for reviews
    # where the source data does not provide an unambiguous product.
    # Therefore only non-null product keys are evaluated.
    # ------------------------------------------------------------------
    review_product_rows = (
        fact_review
        .select("product_key")
        .where(F.col("product_key").isNotNull())
    )

    review_product_evaluated = review_product_rows.count()

    review_product_orphans = (
        review_product_rows
        .join(
            product_keys,
            on="product_key",
            how="left_anti",
        )
        .count()
    )

    results.append(
        run_test(
            "fact_review.product_key has no orphans",
            review_product_evaluated,
            review_product_orphans,
        )
    )

    # ------------------------------------------------------------------
    # 16. Bronze freshness
    # This is a pipeline-level test, not a row-level test.
    # Therefore rows_evaluated is intentionally N/A.
    # ------------------------------------------------------------------
    bronze_customers = read_bronze(
        spark, "customers"
    )

    freshness_result = (
        bronze_customers
        .select(
            F.max("ingestion_ts").alias(
                "latest_ingestion_ts"
            )
        )
        .withColumn(
            "current_ts",
            F.current_timestamp(),
        )
        .withColumn(
            "age_hours",
            (
                F.unix_timestamp("current_ts")
                - F.unix_timestamp("latest_ingestion_ts")
            ) / 3600,
        )
        .collect()[0]
    )

    latest_ingestion = freshness_result[
        "latest_ingestion_ts"
    ]
    age_hours = freshness_result["age_hours"]

    freshness_ok = (
        latest_ingestion is not None
        and age_hours is not None
        and 0 <= age_hours <= FRESHNESS_HOURS
    )

    print(
        f"Latest Bronze ingestion: "
        f"{latest_ingestion}"
    )
    print(
        f"Bronze ingestion age: "
        f"{age_hours:.2f} hours"
    )

    results.append(
        run_test(
            "Bronze ingestion is fresh "
            f"(<= {FRESHNESS_HOURS} hours)",
            None,
            0 if freshness_ok else 1,
        )
    )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print("=" * 100)
    print("DATA QUALITY PROFILING SUMMARY")
    print("=" * 100)

    print(
        f"{'Test':<65} "
        f"{'Evaluated':>12} "
        f"{'Failed':>10} "
        f"{'Failure %':>12}"
    )

    print("-" * 100)

    for result in results:
        evaluated = (
            "N/A"
            if result["rows_evaluated"] is None
            else f"{result['rows_evaluated']:,}"
        )

        failure_pct = (
            "N/A"
            if result["failure_pct"] is None
            else f"{result['failure_pct']:.2f}%"
        )

        print(
            f"{result['test_name']:<65} "
            f"{evaluated:>12} "
            f"{result['rows_failed']:>10,} "
            f"{failure_pct:>12}"
        )

    print("=" * 100)

    passed = sum(result["passed"] for result in results)
    failed = len(results) - passed

    print(f"Tests passed: {passed}")
    print(f"Tests failed: {failed}")

    spark.stop()

    if failed > 0:
        raise RuntimeError(
            f"Data quality validation failed: "
            f"{failed} test(s) failed."
        )

    print("All data quality tests passed")


if __name__ == "__main__":
    main()