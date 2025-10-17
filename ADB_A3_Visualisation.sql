USE ROLE TRAINING_ROLE;
CREATE WAREHOUSE IF NOT EXISTS KOALA_WH INITIALLY_SUSPENDED = TRUE;
USE WAREHOUSE KOALA_WH;

--Analytical quries and visualisations

--1. Average Delay by Restaurant: Bar Chart
-- Use Case: Restaurant Optimisation, Arrival Estimation, Driver allocation
WITH prep_time_per_restaurant AS (
    SELECT
        r.restaurant_key,
        AVG(DATEDIFF(minute, f.order_time, f.pickup_time)) AS avg_prep_time
    FROM
        KOALA_DB.gold.fact_Delivery f
        JOIN KOALA_DB.gold.dim_Restaurant r ON f.restaurant_key = r.restaurant_key
    GROUP BY r.restaurant_key
)
SELECT
    r.restaurant_name,
    AVG(
        DATEDIFF(minute, f.order_time, f.delivery_time) - (p.avg_prep_time + (f.distance_km / 0.67)) --assumed average speed is 0.67 km/min
    ) AS avg_delay_minutes,
    SUM(
        CASE 
            WHEN DATEDIFF(minute, f.order_time, f.delivery_time) > (p.avg_prep_time + (f.distance_km / 0.67)) THEN 1
            ELSE 0 --1: delayed, 0: on time
        END
    ) AS delayed_orders
FROM 
    KOALA_DB.gold.fact_Delivery f
JOIN KOALA_DB.gold.dim_Restaurant r 
    ON f.restaurant_key = r.restaurant_key
JOIN prep_time_per_restaurant p 
    ON f.restaurant_key = p.restaurant_key
GROUP BY r.restaurant_name
    HAVING AVG(
        DATEDIFF(minute, f.order_time, f.delivery_time) - (p.avg_prep_time + (f.distance_km / 0.67)))IS NOT NULL --Rrevent missing values 
ORDER BY avg_delay_minutes DESC;

--2. Average Delivery Duration by Driver: Bar Chart
--Use case: Driver Allocation Optimisation, Arrival Estimation
SELECT
    CONCAT(d.driver_first_name, ' ',d.driver_last_name) AS driver_full_name,
    AVG(DATEDIFF(minute, f.pickup_time, f.delivery_time)) AS avg_delivery_duration,
    COUNT(*) AS total_orders
FROM
    KOALA_DB.gold.fact_Delivery f
    JOIN KOALA_DB.gold.dim_Driver d ON f.driver_key = d.driver_key
GROUP BY
    driver_full_name
ORDER BY
    avg_delivery_duration DESC;
    
--3. Average Preparation Time by Restaurant: Horizontal Bar Chart
--Use Case: Restaurant Optimisation, Arrival Estimation, Driver allocation
SELECT
    r.restaurant_name,
    AVG(DATEDIFF(minute, f.order_time, f.pickup_time)) AS avg_prep_time,
    COUNT(*) AS total_orders
FROM
    KOALA_DB.gold.fact_Delivery f
    JOIN KOALA_DB.gold.dim_Restaurant r ON f.restaurant_key = r.restaurant_key
GROUP BY r.restaurant_name
HAVING AVG(DATEDIFF(minute, f.order_time, f.pickup_time)) IS NOT NULL
ORDER BY avg_prep_time DESC;
    
--4. Delivery Time vs Distance: Scatter/Line Chart
--Use Case: Enhance customer insights 
SELECT
    ROUND(f.distance_km, 1) AS distance_group,
    AVG(DATEDIFF(minute, f.pickup_time, f.delivery_time)) AS avg_delivery_duration
FROM
    KOALA_DB.gold.fact_Delivery f
GROUP BY ROUND(f.distance_km, 1)
HAVING ROUND(f.distance_km, 1) <> 0
AND ROUND(f.distance_km, 1) IS NOT NULL
ORDER BY distance_group;
    
    
--5. Delay Distribution by Time of Day: Line Chart
--Use case: Time estimation, Driver allocation, 
WITH prep_time_per_restaurant AS (
    SELECT
        r.restaurant_key,
        AVG(DATEDIFF(minute, f.order_time, f.pickup_time)) AS avg_prep_time
    FROM
        KOALA_DB.gold.fact_Delivery f
        JOIN KOALA_DB.gold.dim_Restaurant r ON f.restaurant_key = r.restaurant_key
    GROUP BY r.restaurant_key
)
SELECT
    DATE_PART(hour, f.order_time) AS order_hour,
    AVG(
        DATEDIFF(minute, f.order_time, f.delivery_time) -(p.avg_prep_time +(f.distance_km / 0.67))
    ) AS avg_delay_minutes
FROM
    KOALA_DB.gold.fact_Delivery f
    JOIN KOALA_DB.gold.dim_restaurants r ON f.restaurant_key = r.restaurant_key
    JOIN prep_time_per_restaurant p ON f.restaurant_key = p.restaurant_key
GROUP BY order_hour
HAVING DATE_PART(hour, f.order_time) IS NOT NULL
ORDER BY order_hour;

--5. Delay by Region / Suburb: Bar Chart
--Use case: Time estimation, Driver allocation
WITH suburbs AS (
    SELECT
        c.customer_key,
        TRIM(SPLIT_PART(c.customer_address, ',', 2)) AS suburb
    FROM
        KOALA_DB.gold.dim_Customer c
),
prep_time_per_restaurant AS (
    SELECT
        r.restaurant_key,
        AVG(DATEDIFF(minute, f.order_time, f.pickup_time)) AS avg_prep_time
    FROM
        KOALA_DB.gold.fact_Delivery f
        JOIN KOALA_DB.gold.dim_Restaurant r ON f.restaurant_key = r.restaurant_key
    GROUP BY r.restaurant_key
)
SELECT
    s.suburb,
    AVG(
        DATEDIFF(minute, f.order_time, f.delivery_time) -(p.avg_prep_time +(f.distance_km / 0.67))
    ) AS avg_delay_minutes
FROM
    KOALA_DB.gold.fact_Delivery f
    JOIN prep_time_per_restaurant p ON f.restaurant_key = p.restaurant_key
    JOIN suburbs s ON f.customer_key = s.customer_key
GROUP BY s.suburb
ORDER BY avg_delay_minutes DESC;


--6. Relationship between delay and rating by customer: Scatter Chart
--Use case: Business Knowledge
WITH prep_time_per_restaurant AS (
        SELECT
            r.restaurant_key,
            AVG(DATEDIFF('minute', f.order_time, f.pickup_time)) AS avg_prep_time
        FROM
            KOALA_DB.gold.fact_Delivery f
            JOIN KOALA_DB.gold.dim_restaurants r ON f.restaurant_key = r.restaurant_key
        GROUP BY
            r.restaurant_key
    )
SELECT
    f.delID,
    DATEDIFF('minute', f.order_time, f.delivery_time) - (p.avg_prep_time + (f.distance_km / 0.67)) AS delay_minutes,
    f.rating
FROM
    KOALA_DB.gold.fact_Delivery f
    JOIN prep_time_per_restaurant p ON f.restaurant_key = p.restaurant_key
WHERE
    f.rating <> 0
ORDER BY
    delay_minutes DESC;