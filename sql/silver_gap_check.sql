SELECT d::DATE AS missing_day
FROM generate_series(DATE '2026-09-29', DATE '2026-10-28', INTERVAL 1 DAY) AS t(d)
WHERE d::DATE NOT IN (SELECT DISTINCT ds FROM silver.aqi_hourly)
ORDER BY 1;

SELECT count(DISTINCT ds) AS days_loaded, count(*) AS total_rows
FROM silver.aqi_hourly;
