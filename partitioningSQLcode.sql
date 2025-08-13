CREATE USER 'sutthana'@'%' IDENTIFIED BY 'Team6_123!';

GRANT ALL PRIVILEGES ON hotel_data_backup.* TO 'sutthana'@'%';
FLUSH PRIVILEGES;

use hotel_db;

CREATE TABLE hotel_data (
    Booking_ID VARCHAR(50),
    no_of_adults INT,
    no_of_children INT,
    no_of_weekend_nights INT,
    no_of_week_nights INT,
    type_of_meal_plan VARCHAR(100),
    required_car_parking_space INT,
    room_type_reserved VARCHAR(100),
    lead_time INT,
    arrival_year INT,
    arrival_month INT,
    arrival_date INT,
    market_segment_type VARCHAR(100),
    repeated_guest INT,
    no_of_previous_cancellations INT,
    no_of_previous_bookings_not_canceled INT,
    avg_price_per_room FLOAT,
    no_of_special_requests INT,
    booking_status VARCHAR(50)
);

LOAD DATA INFILE '/opt/bitnami/mysql/tmp/hotel_data.csv'
INTO TABLE hotel_data
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


SELECT * FROM hotel_data LIMIT 10;


# 1. Average Room Price for 2017 Bookings
SELECT AVG(avg_price_per_room) AS avg_price_2022
FROM hotel_data
WHERE arrival_year = 2017;

# 2. Count of Bookings per Room Type for 2017
SELECT room_type_reserved, COUNT(*) AS total_bookings
FROM hotel_data
WHERE arrival_year = 2017
GROUP BY room_type_reserved
ORDER BY total_bookings DESC;

# 3. Most Common Booking Month by Market Segment
SELECT market_segment_type, arrival_month, COUNT(*) AS bookings
FROM hotel_data
WHERE arrival_year = 2017
GROUP BY market_segment_type, arrival_month
ORDER BY market_segment_type, bookings DESC;



CREATE TABLE hotel_data_partitioned (
    Booking_ID VARCHAR(50),
    no_of_adults INT,
    no_of_children INT,
    no_of_weekend_nights INT,
    no_of_week_nights INT,
    type_of_meal_plan VARCHAR(100),
    required_car_parking_space INT,
    room_type_reserved VARCHAR(100),
    lead_time INT,
    arrival_year INT,
    arrival_month INT,
    arrival_date INT,
    market_segment_type VARCHAR(100),
    repeated_guest INT,
    no_of_previous_cancellations INT,
    no_of_previous_bookings_not_canceled INT,
    avg_price_per_room FLOAT,
    no_of_special_requests INT,
    booking_status VARCHAR(50)
)
PARTITION BY RANGE (arrival_year) (
    PARTITION p2017 VALUES LESS THAN (2018),
    PARTITION p2018 VALUES LESS THAN (2019),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

INSERT INTO hotel_data_partitioned
SELECT * FROM hotel_data;


# 1. Average Room Price for 2017 Bookings
SELECT AVG(avg_price_per_room) AS avg_price_2022
FROM hotel_data_partitioned
WHERE arrival_year = 2017;

# 2. Count of Bookings per Room Type for 2017
SELECT room_type_reserved, COUNT(*) AS total_bookings
FROM hotel_data_partitioned
WHERE arrival_year = 2017
GROUP BY room_type_reserved
ORDER BY total_bookings DESC;

# 3. Most Common Booking Month by Market Segment
SELECT market_segment_type, arrival_month, COUNT(*) AS bookings
FROM hotel_data_partitioned 
WHERE arrival_year = 2017
GROUP BY market_segment_type, arrival_month
ORDER BY market_segment_type, bookings DESC;


-------------------
# MAKING BIGGER TABLE
INSERT INTO hotel_data
SELECT * FROM hotel_data;

INSERT INTO hotel_data
SELECT * FROM hotel_data;

INSERT INTO hotel_data
SELECT * FROM hotel_data;

INSERT INTO hotel_data
SELECT * FROM hotel_data;

INSERT INTO hotel_data
SELECT * FROM hotel_data;

-- Check the new row count
SELECT COUNT(*) FROM hotel_data;

DROP TABLE IF EXISTS hotel_data_partitioned;

CREATE TABLE hotel_data_partitioned (
    Booking_ID VARCHAR(50),
    no_of_adults INT,
    no_of_children INT,
    no_of_weekend_nights INT,
    no_of_week_nights INT,
    type_of_meal_plan VARCHAR(100),
    required_car_parking_space INT,
    room_type_reserved VARCHAR(100),
    lead_time INT,
    arrival_year INT,
    arrival_month INT,
    arrival_date INT,
    market_segment_type VARCHAR(100),
    repeated_guest INT,
    no_of_previous_cancellations INT,
    no_of_previous_bookings_not_canceled INT,
    avg_price_per_room FLOAT,
    no_of_special_requests INT,
    booking_status VARCHAR(50)
)
PARTITION BY RANGE (arrival_year) (
    PARTITION p2017 VALUES LESS THAN (2018),
    PARTITION p2018 VALUES LESS THAN (2019),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

INSERT INTO hotel_data_partitioned
SELECT * FROM hotel_data;

## 1. Average Room Price for Guests with Special Requests (filtered by year)
SELECT room_type_reserved, AVG(avg_price_per_room) AS avg_price
FROM hotel_data
WHERE arrival_year = 2018
  AND no_of_special_requests >= 2
GROUP BY room_type_reserved
ORDER BY avg_price DESC;

#2. Cancellation Rate for 2018 Only
SELECT room_type_reserved,
       ROUND(SUM(CASE WHEN booking_status = "Canceled" THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS cancellation_rate_percent
FROM hotel_data
WHERE arrival_year = 2018
GROUP BY room_type_reserved
ORDER BY cancellation_rate_percent DESC;

#3. Bookings Distribution by Meal Plan in 2017
SELECT type_of_meal_plan, COUNT(*) AS bookings
FROM hotel_data
WHERE arrival_year = 2017
GROUP BY type_of_meal_plan
ORDER BY bookings DESC;

#4. Find Maximum Lead Time for 2018 Bookings
SELECT MAX(lead_time) AS max_lead_time
FROM hotel_data
WHERE arrival_year = 2018;

#5. Total Revenue Estimate by Market Segment (Price × No of Adults)
SELECT market_segment_type,
       SUM(avg_price_per_room * no_of_adults) AS total_revenue_estimate
FROM hotel_data
WHERE arrival_year = 2017
GROUP BY market_segment_type
ORDER BY total_revenue_estimate DESC;

## 1. Average Room Price for Guests with Special Requests (filtered by year)
SELECT room_type_reserved, AVG(avg_price_per_room) AS avg_price
FROM hotel_data_partitioned
WHERE arrival_year = 2018
  AND no_of_special_requests >= 2
GROUP BY room_type_reserved
ORDER BY avg_price DESC;

#2. Cancellation Rate for 2018 Only
SELECT room_type_reserved,
       ROUND(SUM(CASE WHEN booking_status = 'Canceled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS cancellation_rate_percent
FROM hotel_data_partitioned
WHERE arrival_year = 2018
GROUP BY room_type_reserved
ORDER BY cancellation_rate_percent DESC;

#3. Bookings Distribution by Meal Plan in 2017
SELECT type_of_meal_plan, COUNT(*) AS bookings
FROM hotel_data_partitioned
WHERE arrival_year = 2017
GROUP BY type_of_meal_plan
ORDER BY bookings DESC;

#4. Find Maximum Lead Time for 2018 Bookings
SELECT MAX(lead_time) AS max_lead_time
FROM hotel_data_partitioned
WHERE arrival_year = 2018;

#5. Total Revenue Estimate by Market Segment (Price × No of Adults)
SELECT market_segment_type,
       SUM(avg_price_per_room * no_of_adults) AS total_revenue_estimate
FROM hotel_data_partitioned
WHERE arrival_year = 2017
GROUP BY market_segment_type
ORDER BY total_revenue_estimate DESC;
