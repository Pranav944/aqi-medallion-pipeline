-- 1. RANK: which city is most polluted each day?
SELECT ds, city,
       ROUND(AVG(pm2_5), 1) AS avg_pm25,
       RANK() OVER (PARTITION BY ds ORDER BY AVG(pm2_5) DESC) AS pollution_rank
FROM silver.aqi_hourly
GROUP BY ds, city
ORDER BY ds, pollution_rank;

-- 2. LAG: hour-over-hour change in Delhi
SELECT city, ts, pm2_5,
       LAG(pm2_5) OVER (PARTITION BY city ORDER BY ts) AS prev_hour,
       ROUND(pm2_5 - LAG(pm2_5) OVER (PARTITION BY city ORDER BY ts), 1) AS change
FROM silver.aqi_hourly
WHERE city = 'Delhi' AND ds = '2026-10-28'
ORDER BY ts
LIMIT 8;

-- 3. Rolling 3-hour average for Mumbai
SELECT city, ts, pm2_5,
       ROUND(AVG(pm2_5) OVER (
           PARTITION BY city ORDER BY ts
           ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 1) AS rolling_3h
FROM silver.aqi_hourly
WHERE city = 'Mumbai' AND ds = '2026-10-28'
ORDER BY ts
LIMIT 8;

-- 4. ROW_NUMBER: the worst hour for each city
SELECT ds, city, ts AS peak_hour_utc, pm2_5
FROM silver.aqi_hourly
WHERE ds = '2026-10-28'
QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY pm2_5 DESC) = 1
ORDER BY pm2_5 DESC;