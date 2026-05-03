SELECT
    ROUND(AVG(CASE WHEN is_incomplete_financial_data = 'Y' THEN CAST(1 AS FLOAT) ELSE 0 END), 2) AS is_incomplete_financial_data,
    ROUND(AVG(CASE WHEN err_date_lifecycle = 'Y' THEN CAST(1 AS FLOAT) ELSE 0 END), 2) AS err_date_lifecycle,
    ROUND(AVG(CASE WHEN err_date_sequence = 'Y' THEN CAST(1 AS FLOAT) ELSE 0 END), 2) AS err_date_sequence
FROM
    gold.fact_sales