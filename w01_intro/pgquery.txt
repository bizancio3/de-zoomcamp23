-- Q3) How many taxi trips were totally made on January 15?

SELECT
  COUNT(*)
FROM
  public."green_taxi_data"
WHERE
  (lpep_pickup_datetime>='2019-01-15 00:00:00' AND
  lpep_pickup_datetime<'2019-01-16 00:00:00');


-- Q4) Which was the day with the largest trip distance 
--     Use the pick up time for your calculations

SELECT
  CAST(lpep_pickup_datetime AS DATE) as "day",
  MAX(trip_distance) as "distance"
FROM
  public."green_taxi_data"
GROUP BY
  1
ORDER BY
  "distance" DESC;


-- Q5) In 2019-01-01 how many trips had 2 and 3 passengers?
	
SELECT 
	passenger_count, 
	COUNT(*)
FROM 
	public."green_taxi_data"
WHERE 
	passenger_count IN (2, 3) AND 
	DATE(lpep_pickup_datetime) = '2019-01-01'
GROUP BY 
	passenger_count;


-- Q6) For the passengers picked up in the Astoria Zone 
--     which was the drop off zone that had the largest tip? 
--     We want the name of the zone, not the id.

WITH max_tip AS (
    SELECT 
		MAX(tip_amount) AS max_tip
    FROM 
		public."green_taxi_data" trips
    JOIN 
		public."zones" z 
		ON trips."PULocationID" = z."LocationID"
    WHERE 
		z."Zone" = 'Astoria'
)
SELECT 
	z."Zone", 
	SUM(trips.tip_amount) as total_tips
FROM 
	public."green_taxi_data" trips
JOIN 
	public."zones" z 
	ON trips."DOLocationID" = z."LocationID"
WHERE 
	trips."PULocationID" 
	IN (SELECT "LocationID" from public."zones" where "Zone" = 'Astoria') AND 
	tip_amount = (SELECT max_tip FROM max_tip)
GROUP BY 
	z."Zone";