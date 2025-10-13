USE ROLE TRAINING_ROLE;
USE WAREHOUSE KOALA_WH;

-- Create schemas for each layer (bronze, silver, gold)
CREATE SCHEMA IF NOT EXISTS KOALA_DB.silver;

/*
In the silver layer, data was loaded from the bronze layer and
data quality issues are resolved.
Functions used to resolve the data quality issues include:
- ROW_NUMBER() was used to identify duplicate rows using the natural keys of each table.
- COALESCE() was used to impute missing values.
- IINITCAP() was used to capitalise the first letter of each word.
- UPPER() was used to capitalise every letter of a string
- TRY_TO_DECIMAL() was used to convert longitude and latitude to decimal/return NULL if format is invalid.
*/

--silver.customers
CREATE OR REPLACE TABLE KOALA_DB.silver.Customer AS
SELECT
    cusID,
    INITCAP(TRIM(cusName)) AS cusName,
    COALESCE(TRIM(cusAddress), 'Unknown Address') AS cusAddress
FROM
    (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cusID
            ORDER BY
                cusID
        ) AS rn
    FROM
        KOALA_DB.bronze.Customer
    )
WHERE
    rn = 1 -- remove duplicates
    AND cusID IS NOT NULL
    AND cusName NOT IN ('N/A', ''); -- remove invalid names

SELECT * FROM KOALA_DB.silver.Customer;

--silver.drivers
CREATE OR REPLACE TABLE KOALA_DB.silver.Driver AS
SELECT
    driID,
    INITCAP(TRIM(driName)) AS driName,
    COALESCE(UPPER(TRIM(driVehicle_type)), 'Unknown') AS driVehicle_type,
    COALESCE(driAvg_rating, 0) AS driAvg_rating,
    driShift_start,
    driShift_end,
    LOWER(TRIM(driCurrent_status)) AS driCurrent_status,
    -- normalize case
    TRY_TO_DECIMAL(driLongitude, 10, 5) AS driLongitude,
    -- fix datatype
    TRY_TO_DECIMAL(driLatitude, 10, 5) AS driLatitude
FROM
    (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY driID
            ORDER BY
                driID
            ) AS rn
        FROM
            KOALA_DB.bronze.Driver
    )
WHERE rn = 1
    AND driID IS NOT NULL;

SELECT * FROM KOALA_DB.silver.Driver;

--silver.restaurants
CREATE OR REPLACE TABLE KOALA_DB.silver.Restaurant AS
SELECT
    resID,
    INITCAP(TRIM(resName)) AS resName,
    COALESCE(TRIM(resAddress), 'Unknown Address') AS resAddress,
    COALESCE(resCurrent_queue, 0) AS resCurrent_queue,
    INITCAP(TRIM(resCuisine_type)) AS resCuisine_type
FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY resID
                ORDER BY
                    resID
            ) AS rn
        FROM
            KOALA_DB.bronze.Restaurant
    )
WHERE
    rn = 1
    AND resID IS NOT NULL;

SELECT * FROM KOALA_DB.silver.Restaurant;

--silver.Delivery
CREATE OR REPLACE TABLE KOALA_DB.silver.Delivery AS
SELECT
    delID,
    cusID,
    resID,
    driID,
    delOrder_time,
    delPickup_time,
    delDelivery_time,
    COALESCE(delDistance_km, 0) AS delDistance_km,
    delStatus,
    COALESCE(delRating, 0) AS delRating
FROM
    (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY delID
            ORDER BY
                delOrder_time DESC
        ) AS rn
    FROM
        KOALA_DB.bronze.Delivery
    )
WHERE rn = 1
    AND delID IS NOT NULL
    AND cusID IS NOT NULL
    AND resID IS NOT NULL
    AND driID IS NOT NULL
    AND delOrder_time IS NOT NULL
    AND delPickup_time IS NOT NULL
    AND delDelivery_time IS NOT NULL;

ALTER TABLE KOALA_DB.silver.Customer
ADD PRIMARY KEY (cusID);

ALTER TABLE KOALA_DB.silver.Driver
ADD PRIMARY KEY (driID);

ALTER TABLE KOALA_DB.silver.Restaurant
ADD PRIMARY KEY (resID);

ALTER TABLE KOALA_DB.silver.Delivery
ADD FOREIGN KEY (cusID) REFERENCES KOALA_DB.silver.Customer(cusID);

ALTER TABLE KOALA_DB.silver.Delivery
ADD FOREIGN KEY (driID) REFERENCES KOALA_DB.silver.Driver(driID);

ALTER TABLE KOALA_DB.silver.Delivery
ADD FOREIGN KEY (resID) REFERENCES KOALA_DB.silver.Restaurant(resID);

SELECT * FROM KOALA_DB.silver.Delivery;


--Check for orphaned references (should have no results)
SELECT dl.delID
FROM
    KOALA_DB.silver.Delivery dl
    LEFT JOIN KOALA_DB.silver.Driver dr ON dl.driID = dr.driID
WHERE
    dr.driID IS NULL;
    
--Check of NULLs after cleaning (should be 0)
SELECT COUNT(*) AS null_counts
FROM KOALA_DB.silver.Delivery
WHERE delID IS NULL
    OR cusID IS NULL
    OR resID IS NULL
    OR driID IS NULL;