USE ROLE TRAINING_ROLE;
CREATE WAREHOUSE IF NOT EXISTS KOALA_WH INITIALLY_SUSPENDED = TRUE;
USE WAREHOUSE KOALA_WH;
-- Create schemas for each layer (bronze, silver, gold)
CREATE SCHEMA IF NOT EXISTS KOALA_DB.gold;

/*
Create the gold layer as a star schema
The gold layer is the analytics-ready layer, so referential integrity is
ensured here so that all records in the fact table correctly references existing dimension records.
Surrogate keys are generated using the HASH() function to simplify joins, improve performance,
and allow consistent referencing even if naturla key is duplicated.
The fact table is also linked to the dimension table using both the surrogate keys and natural keys.
This provides flexibility:
    -1. surrogate keys offers better performance and is more robust for analytics.
    -2. Natural keys preserve traceability to the original business data.
*/

-- Dimension Tables
-- DIM_CUSTOMERS
Create OR REPLACE TABLE KOALA_DB.gold.dim_Customer AS
SELECT
    ABS(HASH(cusID)) AS customer_key, --surrogate key
    cusID,
    cusFname AS customer_first_name,
    cusLname AS customer_last_name,
    cusAddress AS customer_address
FROM
    KOALA_DB.silver.Customer;

SELECT * FROM KOALA_DB.gold.dim_Customer;

-- DIM_DRIVERS
CREATE OR REPLACE TABLE KOALA_DB.gold.dim_Driver AS
SELECT
    ABS(HASH(driID)) AS driver_key,
    driID,
    driFname AS driver_first_name,
    driLname AS driver_last_name,
    driVehicle_type AS vehicle_type,
    driAvg_rating AS avg_rating,
    driShift_start AS shift_start,
    driShift_end AS shift_end,
    driCurrent_status AS current_status,
    driLongitude AS longitude,
    driLatitude AS latitude
FROM
    KOALA_DB.silver.Driver;

SELECT * FROM KOALA_DB.gold.dim_Driver;

--DIM_RESTAURANTS
CREATE OR REPLACE TABLE KOALA_DB.gold.dim_Restaurant AS
SELECT
    ABS(HASH(resID)) AS restaurant_key,
    resID,
    resName AS restaurant_name,
    resAddress AS restaurant_address,
    resCurrent_queue AS current_queue,
    resCuisine_type AS cuisine_type
FROM
    KOALA_DB.silver.Restaurant;

SELECT * FROM KOALA_DB.gold.dim_Restaurant;

--FACT_ORDERS
CREATE OR REPLACE TABLE KOALA_DB.gold.fact_Delivery AS
SELECT
    ABS(HASH(delID)) AS delivery_key,
    dl.delID,
    c.customer_key,
    r.restaurant_key,
    d.driver_key,
    dl.delOrder_time AS order_time,
    dl.delPickup_time AS pickup_time,
    dl.delDelivery_time AS delivery_time,
    dl.delDistance_km AS distance_km,
    dl.delStatus AS status,
    dl.delRating AS rating
FROM
    KOALA_DB.silver.Delivery dl
    LEFT JOIN KOALA_DB.gold.dim_Customer c ON dl.cusID = c.cusID
    LEFT JOIN KOALA_DB.gold.dim_Driver d ON dl.driID = d.driID
    LEFT JOIN KOALA_DB.gold.dim_Restaurant r ON dl.resID = r.resID;
    
ALTER TABLE KOALA_DB.gold.fact_Delivery
ADD PRIMARY KEY (delivery_key);

ALTER TABLE KOALA_DB.gold.dim_Customer
ADD PRIMARY KEY (customer_key);

ALTER TABLE KOALA_DB.gold.dim_Driver
ADD PRIMARY KEY (driver_key);

ALTER TABLE KOALA_DB.gold.dim_Restaurant
ADD PRIMARY KEY (restaurant_key);

ALTER TABLE KOALA_DB.gold.fact_Delivery
ADD FOREIGN KEY (customer_key) REFERENCES KOALA_DB.gold.dim_Customer(customer_key);

ALTER TABLE KOALA_DB.gold.fact_Delivery
ADD FOREIGN KEY (driver_key) REFERENCES KOALA_DB.gold.dim_Driver(driver_key);

ALTER TABLE KOALA_DB.gold.fact_Delivery
ADD FOREIGN KEY (restaurant_key) REFERENCES KOALA_DB.gold.dim_Restaurant(restaurant_key);

SELECT * FROM KOALA_DB.gold.fact_Delivery;
