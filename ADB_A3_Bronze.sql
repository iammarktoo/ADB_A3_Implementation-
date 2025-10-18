USE ROLE TRAINING_ROLE;
CREATE WAREHOUSE IF NOT EXISTS KOALA_WH INITIALLY_SUSPENDED = TRUE;
USE WAREHOUSE KOALA_WH;

-- Create schemas for each layer (bronze, silver, gold)
CREATE SCHEMA IF NOT EXISTS KOALA_DB.bronze;

/*
Create bronze layer tables & insert data:
In this implementation, the bronze layer represents a raw data ingestion zone,
storing unprocessed data inputted from various data sources.
The bronze focuses on rapid data ingestion and data completeness rather than quality.
As a result, the bronze layer may contain data quality issues such as
missing values, misaligned case formats, and duplicates.
Additionally, the bronze layer does not enforce referential integrity.
Since completeness is the main goal of this layer, enforcing
referential integrity at such an early stage may result in ingestion
failures and delay the pipeline. Instead, referential integrity was enforced
in the gold layer, once the data is standardised.
*/

CREATE
OR REPLACE TABLE KOALA_DB.bronze.Delivery(
    delID VARCHAR(5),
    cusID VARCHAR(5),
    resID VARCHAR(5),
    driID VARCHAR(5),
    delOrder_time TIMESTAMP_NTZ,
    delPickup_time TIMESTAMP_NTZ,
    delDelivery_time TIMESTAMP_NTZ,
    delDistance_km NUMBER,
    delStatus BOOLEAN,
    delRating INTEGER
);

CREATE
OR REPLACE TABLE KOALA_DB.bronze.Driver(
    driID VARCHAR(5),
    driName VARCHAR(30),
    driVehicle_type STRING,
    driAvg_rating NUMBER,
    driShift_start TIME,
    driShift_end TIME,
    driCurrent_status STRING,
    driLongitude VARCHAR (10),
    driLatitude VARCHAR(10)
);

CREATE
OR REPLACE TABLE KOALA_DB.bronze.Restaurant(
    resID VARCHAR(5),
    resName VARCHAR(30),
    resAddress VARCHAR(140),
    resCurrent_queue NUMBER,
    resCuisine_type STRING
);

CREATE
OR REPLACE TABLE KOALA_DB.bronze.Customer(
    cusID VARCHAR(5),
    cusName VARCHAR(30),
    cusAddress VARCHAR(140)
);

-- Insert data into bronze.orders
INSERT INTO
    KOALA_DB.bronze.Delivery
VALUES
    ('O001','C001','R001','D001','2025-10-01 10:00:00','2025-10-01 10:10:00','2025-10-01 10:25:00',5.4,TRUE,5),
    ('O002','C002','R002','D002','2025-10-01 11:00:00','2025-10-01 11:20:00','2025-10-01 12:00:00',8.1,FALSE,4),
    ('O003','C003','R003','D003','2025-10-01 12:00:00','2025-10-01 12:15:00','2025-10-01 12:45:00',3.2,TRUE,3),
    ('O004','C004','R004','D004','2025-10-01 13:00:00','2025-10-01 13:25:00','2025-10-01 14:15:00',9.5,TRUE,5),
    ('O005','C005','R005','D005','2025-10-01 14:00:00','2025-10-01 14:18:00','2025-10-01 14:43:00',NULL,FALSE,2), -- Missing distance
    ('O006','C006','R006','D006',NULL,'2025-10-01 15:10:00','2025-10-01 15:40:00',4.5,TRUE,4), -- Missing order time
    ('O007','C007','R007','D007','2025-10-01 16:00:00','2025-10-01 16:10:00','2025-10-01 16:25:00',1.9,TRUE,NULL), -- Missing rating
    ('O008','C008','R008','D008','2025-10-01 17:00:00','2025-10-01 17:10:00',NULL,6.3,TRUE,3), -- Missing delivery time
    ('O009','C009','R009','D009','2025-10-01 18:00:00','2025-10-01 18:10:00','2025-10-01 18:55:00',7.0,TRUE,5),
    ('O010','C010','R010','D010','2025-10-01 19:00:00','2025-10-01 19:10:00','2025-10-01 19:45:00',2.4,TRUE,4),
    ('O002','C002','R002','D002','2025-10-01 11:00:00','2025-10-01 11:20:00','2025-10-01 12:00:00',8.1,FALSE,4), -- Duplicate
    ('O011','C011','R011','D011','2025-10-01 20:00:00','2025-10-01 20:10:00','2025-10-01 20:30:00',3.9,TRUE,5),
    ('O012','C012','R012','D012','2025-10-01 21:00:00','2025-10-01 21:15:00','2025-10-01 22:15:00',10.0,FALSE,2),
    ('O013','C013','R013','D013','2025-10-01 22:00:00','2025-10-01 22:20:00','2025-10-01 23:15:00',8.7,TRUE,5),
    ('O014','C014','R014','D014','2025-10-01 23:00:00','2025-10-01 23:18:00','2025-10-01 23:53:00',4.0,TRUE,3),
    ('O015','C015','R015','D015','2025-10-01 10:30:00','2025-10-01 10:45:00','2025-10-01 11:20:00',6.9,TRUE,2),
    ('O016','C016','R016','D016','2025-10-01 11:45:00','2025-10-01 12:05:00','2025-10-01 12:45:00',5.0,TRUE,4),
    ('O017','C017','R017','D017','2025-10-01 13:10:00','2025-10-01 13:30:00','2025-10-01 14:10:00',9.2,TRUE,5),
    ('O018','C018','R018','D018','2025-10-01 14:25:00','2025-10-01 14:45:00','2025-10-01 15:05:00',2.8,TRUE,1),
    ('O019', NULL, 'R019', 'D019', '2025-10-01 15:25:00', '2025-10-01 15:45:00', '2025-10-01 16:25:00', 3.4, TRUE, 3); -- Missing customer_id

SELECT * FROM KOALA_DB.bronze.Delivery;

-- Insert data into bronze.drivers
INSERT INTO
    KOALA_DB.bronze.Driver
VALUES
    ('D001','Alice Johnson','Car',4.9,'08:00','16:00','active','151.21','-33.87'),
    ('D002','Bob Lee','Motorbike',4.7,'09:00','17:00','active','151.18','-33.89'),
    ('D003','Charlie Kim','Car',NULL,'10:00','18:00','inactive','151.19','-33.88'),-- Missing avg_rating
    ('D004','David Chan','Bicycle',4.1,'07:00','15:00','active','151.22','-33.86'),
    ('D005','Eve Wang','Car',4.5,'08:30','16:30','active','151.17','-33.84'),
    ('D006','Fiona Liu','Car',4.3,'09:00','17:00','inactive','151.17','-33.87'),
    ('D007','George Smith','Van',4.8,'10:00','18:00','active','151.15','-33.83'),
    ('D008','Hannah Tan','CAR',4.6,'11:00','19:00','active','151.19','-33.83'),-- Inconsistent case
    ('D009','Ian Wong','Motorbike',4.0,'12:00','20:00','ACTIVE','151.18','-33.82'),
    -- Inconsistent status case
    ('D010','Jake Lim','Car',4.2,'13:00','21:00','active','151.23','-33.89'),
    ('D002','Bob Lee','Motorbike',4.7,'09:00','17:00','active','151.18','-33.89'),
    -- Duplicate
    ('D011','Karen Yu','Car',4.1,'14:00','22:00','inactive','151.20','-33.81'),
    ('D012','Leo Park','Bicycle',NULL,'15:00','23:00','active','151.21','-33.80'), --Missing avg_rating,
    ('D013','Mia Zhang','Van',4.4,'08:00','16:00','active','151.14','-33.79'),
    ('D014','Noah Lee','Car',4.9,'09:30','17:30','inactive','151.25','-33.90'),
    ('D015','Olivia Chen','Motorbike',4.0,'10:30','18:30','active','151.26','-33.91'),
    ('D016','Peter Lau',NULL,3.8,'11:00','19:00','active','151.22','-33.92'),
    -- Missing vehicle type
    ('D017','Queenie Goh','Car',4.7,'12:00','20:00','active','151.27','-33.93'),
    ('D018','Ryan Lee','Van',4.5,'13:00','21:00','active','151.28','-33.94'),
    ('D019','Sarah Lim','Bicycle',4.6,'14:00','22:00','inactive','151.29','-33.95');

SELECT * FROM KOALA_DB.bronze.Driver;

-- Insert data into bronze.restaurants
INSERT INTO
    KOALA_DB.bronze.Restaurant
VALUES
    ('R001','Sushi Zen','123 King St, Sydney CBD',3,'Japanese'),
    ('R002','Burger Hub','45 Queen St, Newtown',5,'Fast Food'),
    ('R003','Pasta House','12 Crown St, Surry Hills',2,'Italian'),
    ('R004','Taco Fiesta','99 Pitt St, Sydney CBD',4,'Mexican'),
    ('R005','Curry Palace','22 York St, The Rocks',6,'Indian'),
    ('R006','Dragon Wok','88 George St, Haymarket',4,'Chinese'),
    ('R007','Pizza Town','10 Market St, Darling Harbour',3,'Italian'),
    ('R008','Burger Hub','45 Queen St, Newtown',5,'Fast Food'),
    -- Duplicate
    ('R009','Green Garden','9 Oxford St, Paddington',2,'Vegetarian'),
    ('R010','Sweet Treats','101 Sussex St, Barangaroo',1,'Desserts'),
    ('R011', 'Café de Paris', NULL, 2, 'French'),
    -- Missing address
    ('R012','Kebab Express','77 Liverpool St, Haymarket',NULL,'Turkish'),
    -- Missing queue
    ('R013','Curry Palace','22 York St, The Rocks',6,'Indian'),
    -- Duplicate
    ('R014','Bento GO','55 Bathurst St, Darling Harbour',3,'Japanese'),
    ('R015','Veggie Love','33 Goulburn St, Chinatown',2,'Vegetarian'),
    ('R016','Noodle Bar','14 Kent St, Wynyard',5,'Chinese'),
    ('R017','Ramen Master','9 Bridge St, Circular Quay',4,'Japanese'),
    ('R018','Falafel Town','3 Elizabeth St, Surry Hills',3,'Middle Eastern'),
    ('R019','Steak House','20 Park St, Sydney CBD',5,'Western'),
    ('R020','Ocean Catch','8 Harbour St, Darling Harbour',4,'Seafood');
    
SELECT * FROM KOALA_DB.bronze.Restaurant;

-- Insert data into bronze.customers
INSERT INTO
    KOALA_DB.bronze.Customer
VALUES
    ('C001', 'John Doe', '5 Kent St, Sydney CBD'),
    ('C002', 'Jane Smith', '7 Pitt St, Sydney CBD'),
    ('C003', 'Tom Brown', '8 Market St, Sydney CBD'),
    ('C004', 'Lisa Wong', '12 York St, The Rocks'),
    ('C005','Michael Tan','15 George St, Darling Harbour'),
    ('C006', 'Emily Chen', NULL),-- Missing address
    ('C007', 'Chris Lee', '20 Oxford St, Paddington'),
    ('C008', 'Rachel Lim', '22 Liverpool St, Haymarket'),
    ('C009','Andy Park','25 Castlereagh St, Sydney CBD'),
    ('C010','Susan Chan','30 Bridge St, Circular Quay'),
    ('C002', 'Jane Smith', '7 Pitt St, Sydney CBD'),-- Duplicate
    ('C011','Daniel Wu','33 Elizabeth St, Surry Hills'),
    ('C012','Grace Kim','35 Bathurst St, Darling Harbour'),
    ('C013', 'Oliver Li', '40 Clarence St, Barangaroo'),
    ('C014', 'Sophia Zhang', '45 King St, Sydney CBD'),
    ('C015', 'Henry Cho', '50 Sussex St, Pyrmont'),
    ('C016', 'Jessica Lee', '55 Pitt St, Sydney CBD'),
    ('C017', 'Ryan Chen', '60 York St, Wynyard'),
    ('C018', 'Helen Ma', '65 George St, The Rocks'),
    ('C019', 'N/A', NULL); -- Invalid name and missing address
SELECT * FROM KOALA_DB.bronze.Customer;

--Total missing values per table (Bronze Layer)
SELECT 
    'Delivery' AS table_name,
    (
        SUM(CASE WHEN delID IS NULL THEN 1 ELSE 0 END)+
        SUM(CASE WHEN cusID IS NULL THEN 1 ELSE 0 END)+
        SUM(CASE WHEN resID IS NULL THEN 1 ELSE 0 END)+
        SUM(CASE WHEN driID IS NULL THEN 1 ELSE 0 END)+
        SUM(CASE WHEN delOrder_time IS NULL THEN 1 ELSE 0 END)+
        SUM(CASE WHEN delPickup_time IS NULL THEN 1 ELSE 0 END)+
        SUM(CASE WHEN delDelivery_time IS NULL THEN 1 ELSE 0 END)+
        SUM(CASE WHEN delDistance_km IS NULL THEN 1 ELSE 0 END)+
        SUM(CASE WHEN delStatus IS NULL THEN 1 ELSE 0 END)+
        SUM(CASE WHEN delRating IS NULL THEN 1 ELSE 0 END)
    ) AS total_nulls
FROM KOALA_DB.bronze.Delivery

UNION ALL

SELECT
    'Driver' AS table_name,
    (
        SUM (CASE WHEN driID IS NULL THEN 1 ELSE 0 END)+
        SUM (CASE WHEN driName IS NULL THEN 1 ELSE 0 END)+
        SUM (CASE WHEN driVehicle_type IS NULL THEN 1 ELSE 0 END)+
        SUM (CASE WHEN driAvg_rating IS NULL THEN 1 ELSE 0 END)+
        SUM (CASE WHEN driShift_start IS NULL THEN 1 ELSE 0 END)+
        SUM (CASE WHEN driShift_end IS NULL THEN 1 ELSE 0 END)+
        SUM (CASE WHEN driCurrent_status IS NULL THEN 1 ELSE 0 END)+
        SUM (CASE WHEN driLongitude IS NULL THEN 1 ELSE 0 END)+
        SUM (CASE WHEN driLatitude IS NULL THEN 1 ELSE 0 END)
        
    ) AS total_nulls
FROM KOALA_DB.bronze.Driver

UNION ALL

SELECT 
  'Restaurant' AS table_name,
  (
    SUM(CASE WHEN resID IS NULL THEN 1 ELSE 0 END)+
    SUM(CASE WHEN resName IS NULL THEN 1 ELSE 0 END)+
    SUM(CASE WHEN resAddress IS NULL THEN 1 ELSE 0 END)+
    SUM(CASE WHEN resCurrent_queue IS NULL THEN 1 ELSE 0 END)+
    SUM(CASE WHEN resCuisine_type IS NULL THEN 1 ELSE 0 END)
  ) AS total_nulls
FROM KOALA_DB.bronze.Restaurant

UNION ALL

SELECT 
  'Customer' AS table_name,
  (
    SUM(CASE WHEN cusID IS NULL THEN 1 ELSE 0 END)+
    SUM(CASE WHEN cusName  IS NULL THEN 1 ELSE 0 END)+
    SUM(CASE WHEN cusAddress  IS NULL THEN 1 ELSE 0 END)
  ) AS total_nulls
FROM KOALA_DB.bronze.Customer;