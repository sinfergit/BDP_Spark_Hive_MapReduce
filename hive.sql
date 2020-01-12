----creating directory to store dataset(listings.csv)
--hdfs dfs -mkdir /singaHiveData
--pwd
--hdfs dfs -put listingsoriginal.csv /singaHiveData/
--hive
CREATE DATABASE airbnbhive;
USE airbnbhive;


--select the count of rows 
select count(*) from bnbSin;

--create external table
CREATE EXTERNAL TABLE bnbSin
(
    id INT,
    name STRING, 
    host_id INT, 
    host_name STRING, 
    neighborhood_group STRING, 
    neighborhood STRING, 
    latitude FLOAT, 
    longtitude FLOAT, 
    roomtype STRING, 
    price INT, 
    minimum_nights INT, 
    number_of_reviews INT, 
    last_review DATE, 
    reviews_per_month FLOAT, 
    calculated_host_listing_count INT, 
    availability_365 INT
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    LOCATION '/singaHiveData/'
    TBLPROPERTIES ("skip.header.line.count"="1"
    );

-- create another table with casting
CREATE TABLE singa_data as 
SELECT 
    cast(id as int) as id, 
    cast(host_id as int) as host_id,
    cast(neighborhood_group as string) as neighborhood_group, 
    cast(neighborhood as string) as neighborhood, 
    cast(latitude as float) as latitude, 
    cast(longtitude as float) as longtitude,
    cast(roomtype as string) as roomtype, 
    cast(price as int) as price, 
    cast(minimum_nights as int) as minimum_nights, 
    cast(number_of_reviews as int) as number_of_reviews, 
    cast(last_review as date) as last_review, 
    cast(reviews_per_month as float) as reviews_per_month, 
    cast(calculated_host_listing_count as int) as calculated_host_listing_count, 
    cast(availability_365 as int) as availability_365 from bnbSin;



-- Average price of Private room rental for neighbourhood_group
select neighborhood_group, avg(price) as avgPrice
from singa_data where roomtype = 'Private room' 
group by neighborhood_group sort by avgPrice;

-- Top 10 neighbourhood based on Average price of Private room.
select neighborhood, avg_ from ( select neighborhood, avg(price) as avg_ 
from singa_data where roomtype = 'Private room' 
group by neighborhood) bnbSin order by avg_ desc limit 10;

-- The 5 lowest price properties per each room_type.
SELECT host_id, price, roomtype FROM (SELECT ROW_NUMBER()OVER(PARTITION BY 
roomtype ORDER BY price ASC) AS price_range, * FROM singa_data) x WHERE 
price is not NULL AND price_range IN (1,2,3,4,5);