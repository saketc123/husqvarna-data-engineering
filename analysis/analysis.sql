-- ============================================================
-- HUSQVARNA MARKETPLACE ANALYTICAL LAYER
-- Spark SQL
-- ============================================================


-- ============================================================
-- Q1
-- Which seller × product-category combinations have the
-- highest late-delivery rates recently?
--
-- "Recently" = last 90 days relative to the latest purchase
-- date available in the dataset.
-- ============================================================

WITH latest_date AS (
    SELECT MAX(CAST(purchased_at AS DATE)) AS max_purchase_date
    FROM fact_order_item
),

recent_delivery AS (
    SELECT
        foi.seller_key,
        foi.product_key,
        foi.delivered_at,
        foi.est_delivery_date
    FROM fact_order_item foi
    CROSS JOIN latest_date d
    WHERE CAST(foi.purchased_at AS DATE)
          >= DATE_SUB(d.max_purchase_date, 90)
        AND foi.delivered_at IS NOT NULL
        AND foi.est_delivery_date IS NOT NULL
)

SELECT
    ds.seller_id,
    dp.category_family,
    COUNT(*) AS delivered_items,
    SUM(
        CASE
            WHEN rd.delivered_at > rd.est_delivery_date
            THEN 1
            ELSE 0
        END
    ) AS late_items,
    ROUND(
        100.0 *
        SUM(
            CASE
                WHEN rd.delivered_at > rd.est_delivery_date
                THEN 1
                ELSE 0
            END
        ) / COUNT(*),
        2
    ) AS late_delivery_rate_pct
FROM recent_delivery rd
JOIN dim_seller ds
    ON rd.seller_key = ds.seller_key
JOIN dim_product dp
    ON rd.product_key = dp.product_key
WHERE dp.category_family IS NOT NULL
GROUP BY
    ds.seller_id,
    dp.category_family
HAVING COUNT(*) >= 5
ORDER BY
    late_delivery_rate_pct DESC,
    delivered_items DESC;


-- ============================================================
-- Q2
-- What is the average purchase-to-delivery lag by state?
--
-- State = customer/destination state.
-- ============================================================

SELECT
    dc.customer_state,
    ROUND(
        AVG(
            DATEDIFF(
                CAST(foi.delivered_at AS DATE),
                CAST(foi.purchased_at AS DATE)
            )
        ),
        2
    ) AS average_purchase_to_delivery_days,
    COUNT(*) AS delivered_items
FROM fact_order_item foi
JOIN dim_customer dc
    ON foi.customer_key = dc.customer_key
WHERE foi.purchased_at IS NOT NULL
  AND foi.delivered_at IS NOT NULL
GROUP BY
    dc.customer_state
ORDER BY
    average_purchase_to_delivery_days DESC;


-- ============================================================
-- Q3
-- Which product categories show the sharpest month-over-month
-- rise in negative reviews (score <= 2)?
--
-- Reviews are order-level. A review is associated with the
-- product(s) contained in its order. DISTINCT review_id
-- prevents duplicate counting within a category.
--
-- "Sharpest rise" = largest percentage increase from the
-- previous month.
-- ============================================================

WITH monthly_negative_reviews AS (
    SELECT
        DATE_TRUNC(
            'month',
            fr.review_creation_date
        ) AS review_month,
        fr.category_family,
        COUNT(
            DISTINCT CASE
                WHEN fr.review_score <= 2
                THEN fr.review_id
            END
        ) AS negative_reviews
    FROM fact_review fr
    WHERE fr.review_score IS NOT NULL
      AND fr.category_family IS NOT NULL
    GROUP BY
        DATE_TRUNC(
            'month',
            fr.review_creation_date
        ),
        fr.category_family
),

with_previous_month AS (
    SELECT
        review_month,
        category_family,
        negative_reviews,
        LAG(negative_reviews) OVER (
            PARTITION BY category_family
            ORDER BY review_month
        ) AS previous_month_negative_reviews
    FROM monthly_negative_reviews
)

SELECT
    review_month,
    category_family,
    negative_reviews,
    previous_month_negative_reviews,
    negative_reviews
        - previous_month_negative_reviews
        AS absolute_change,
    ROUND(
        100.0 *
        (
            negative_reviews
            - previous_month_negative_reviews
        )
        / previous_month_negative_reviews,
        2
    ) AS mom_change_pct
FROM with_previous_month
WHERE previous_month_negative_reviews > 0
ORDER BY
    mom_change_pct DESC;


-- ============================================================
-- Q4
-- Which orders have anomalous total value
-- (item_price + freight_cost) versus their category's p95?
--
-- For multi-category orders, the baseline is calculated at the
-- order × category level.
-- ============================================================

WITH order_category_value AS (
    SELECT
        foi.order_id,
        dp.category_family,
        SUM(
            foi.item_price + foi.freight_cost
        ) AS order_category_value
    FROM fact_order_item foi
    JOIN dim_product dp
        ON foi.product_key = dp.product_key
    WHERE dp.category_family IS NOT NULL
    GROUP BY
        foi.order_id,
        dp.category_family
),

category_p95 AS (
    SELECT
        category_family,
        PERCENTILE_APPROX(
            order_category_value,
            0.95
        ) AS category_p95_value
    FROM order_category_value
    GROUP BY
        category_family
)

SELECT
    ocv.order_id,
    ocv.category_family,
    ROUND(
        ocv.order_category_value,
        2
    ) AS order_category_value,
    ROUND(
        cp.category_p95_value,
        2
    ) AS category_p95_value,
    ROUND(
        ocv.order_category_value
        / cp.category_p95_value,
        2
    ) AS multiple_of_category_p95
FROM order_category_value ocv
JOIN category_p95 cp
    ON ocv.category_family = cp.category_family
WHERE ocv.order_category_value > cp.category_p95_value
ORDER BY
    multiple_of_category_p95 DESC;


-- ============================================================
-- Q5 — OUR ADDITIONAL QUESTION
-- Which product categories generate the most revenue and units?
-- ============================================================

SELECT
    dp.category_family,
    COUNT(DISTINCT foi.order_id) AS orders,
    COUNT(*) AS units_sold,
    ROUND(
        SUM(foi.item_price),
        2
    ) AS item_revenue,
    ROUND(
        SUM(foi.freight_cost),
        2
    ) AS freight_cost,
    ROUND(
        SUM(
            foi.item_price + foi.freight_cost
        ),
        2
    ) AS total_value
FROM fact_order_item foi
JOIN dim_product dp
    ON foi.product_key = dp.product_key
WHERE dp.category_family IS NOT NULL
GROUP BY
    dp.category_family
ORDER BY
    total_value DESC;