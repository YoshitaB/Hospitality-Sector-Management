CREATE TABLE fact_bookings (
    booking_id VARCHAR(100) PRIMARY KEY,
	property_id INT,
	booking_date DATE,
	check_in_date DATE,
	checkout_date DATE,
	no_guests INT,
	room_category VARCHAR(20),
	booking_platform CHAR(100),
	ratings_given DECIMAL(10, 2),
	booking_status CHAR(50),
	revenue_generated INT,
	revenue_realized INT

);


SELECT * FROM fact_bookings;


CREATE TABLE fact_aggregated_bookings (
	property_id INT,
	check_in_date DATE,
	room_category VARCHAR(50),
	successful_bookings INT,
	capacity INT

);

CREATE TABLE dim_hotels (
	property_id INT,
	property_name CHAR(50),
	category CHAR(50),
	city CHAR(50)

);

CREATE TABLE dim_date (
	date DATE,
	mmm_yy VARCHAR(50),
	week_no VARCHAR(40),
	day_type CHAR(30)

);


CREATE TABLE dim_rooms (
	room_id	VARCHAR(10),
	room_class CHAR(40)

);


SELECT * FROM dim_rooms;
SELECT * FROM dim_date;
SELECT * FROM dim_hotels;
SELECT * FROM fact_aggregated_bookings;
SELECT * FROM fact_bookings;

-- Objective: Combine booking facts with date and room details  
-- Outcome: Returns all booking records enriched with calendar attributes and room information 

SELECT *
FROM fact_aggregated_bookings f
JOIN dim_date d
ON f.check_in_date = d.date
JOIN dim_rooms r
ON r.room_id = f.room_category;


-- Explore room performance segmented by weekdays vs weekends
-- Objective: Compare booking trends, capacity utilization, and vacancy by room type.
SELECT d.day_type,r.room_class, SUM(f.successful_bookings) total_bookings , SUM(capacity) total_capacity, 100-(SUM(successful_bookings)*100.0/SUM(capacity)) vacancy
FROM fact_aggregated_bookings f
JOIN dim_date d
ON f.check_in_date = d.date
JOIN dim_rooms r
ON r.room_id = f.room_category
GROUP BY d.day_type,r.room_class
ORDER BY r.room_class,day_type;



-- GAP ONE
-- Objective: Measure occupancy, ADR, and RevPAR by weekdays vs weekends.
-- Insight: Understand performance variation between business days and leisure days.

WITH base AS (
  SELECT
    fab.property_id,
    fab.check_in_date,
    fab.room_category,
    fab.successful_bookings,
    fab.capacity,
    dd.day_type,         -- 'Weekday' or 'Weekend'
    dd.mmm_yy
  FROM fact_aggregated_bookings fab
  JOIN dim_date dd
    ON fab.check_in_date = dd.date
),
-- bring in realized revenue per (property, date, room_category) from booking-level facts
rev AS (
  SELECT
    fb.property_id,
    fb.check_in_date,
    fb.room_category,
    SUM(fb.revenue_realized) AS realized_rev
  FROM fact_bookings fb
  -- keep all bookings; realized revenue already nets out cancels if your ETL does that
  GROUP BY 1,2,3
),
merged AS (
  SELECT
    b.*,
    COALESCE(r.realized_rev, 0) AS realized_rev,
    CASE WHEN b.capacity > 0 THEN b.successful_bookings*1.0 / b.capacity ELSE NULL END AS occupancy,
    CASE WHEN b.successful_bookings > 0 THEN COALESCE(r.realized_rev,0)*1.0 / b.successful_bookings ELSE NULL END AS adr,
CASE WHEN b.capacity > 0 THEN COALESCE(r.realized_rev,0)*1.0 / b.capacity ELSE NULL END AS revpar
  FROM base b
  LEFT JOIN rev r
    ON r.property_id = b.property_id
   AND r.check_in_date = b.check_in_date
   AND r.room_category = b.room_category
)
SELECT
  day_type,
  AVG(occupancy) AS avg_occupancy,
  AVG(adr)       AS avg_adr,
  AVG(revpar)    AS avg_revpar
FROM merged
GROUP BY day_type
ORDER BY day_type;


-- GAP TWO
-- Objective: Compare expected revenue (generated) vs actual revenue (realized).
-- Insight: Quantifies revenue leakage and realization ratios by room category.

SELECT
  fb.room_category,
  SUM(fb.revenue_generated) AS generated_revenue,
  SUM(fb.revenue_realized)  AS realized_revenue,
  SUM(fb.revenue_generated) - SUM(fb.revenue_realized) AS shortfall,
  CASE WHEN SUM(fb.revenue_generated) = 0 THEN NULL
       ELSE SUM(fb.revenue_realized)*1.0 / SUM(fb.revenue_generated)
  END AS realization_ratio
FROM fact_bookings fb
GROUP BY fb.room_category
ORDER BY shortfall DESC;

-- GAP THREE PART 1
-- Objective: Identify platforms in Delhi with the highest cancellation rates.
-- Insight: Spot problem platforms contributing most to cancellations.

WITH labeled AS (
  SELECT
    h.city,
    fb.booking_platform,
    fb.booking_status,
    CASE 
      WHEN LOWER(fb.booking_status) LIKE '%cancel%' THEN 1
      ELSE 0
    END AS is_cancel
  FROM fact_bookings fb
  JOIN dim_hotels h
    ON h.property_id = fb.property_id
)
SELECT
  city,
  booking_platform,
  SUM(is_cancel)*100.0 / COALESCE(COUNT(*),0) AS cancel_rate_pct,
  COUNT(*) AS total_bookings
FROM labeled
WHERE city = 'Delhi'
GROUP BY city, booking_platform
ORDER BY cancel_rate_pct DESC;


-- GAP THREE PART 2
-- Objective: Analyze how cancellation rates differ by lead time across platforms in Delhi.
-- Insight: Detect if cancellations are more likely for short-notice or long-lead bookings.

WITH delhi AS (
  SELECT
    fb.booking_platform,
    (fb.check_in_date - fb.booking_date) AS lead_days, -- difference in days
    CASE 
      WHEN LOWER(fb.booking_status) LIKE '%cancel%' THEN 1 
      ELSE 0 
    END AS is_cancel
  FROM fact_bookings fb
  JOIN dim_hotels h
    ON h.property_id = fb.property_id
  WHERE h.city = 'Delhi'
),
bucketed AS (
  SELECT
    booking_platform,
    CASE
      WHEN lead_days <= 2  THEN '0-2'
      WHEN lead_days <= 7  THEN '3-7'
      WHEN lead_days <= 14 THEN '8-14'
      ELSE '15+'
    END AS lead_bucket,
    is_cancel
  FROM delhi
)
SELECT
  booking_platform,
  lead_bucket,
  AVG(is_cancel) * 100 AS cancel_rate_pct,
  COUNT(*) AS bookings
FROM bucketed
GROUP BY booking_platform, lead_bucket
ORDER BY booking_platform, lead_bucket;


-- GAP FOUR
-- Objective: Track monthly performance trends.
-- Metrics: Rooms sold, ADR, RevPAR, and occupancy.
-- Insight: Identifies demand seasonality and monthly revenue health.


WITH base AS (
  SELECT
    fab.property_id,
    fab.check_in_date,
    fab.room_category,
    fab.successful_bookings,
    fab.capacity,
    dd.mmm_yy AS month_label
  FROM fact_aggregated_bookings fab
  JOIN dim_date dd
    ON fab.check_in_date = dd.date
),
rev AS (
  SELECT
    fb.property_id,
    fb.check_in_date,
    fb.room_category,
    SUM(fb.revenue_realized) AS realized_rev
  FROM fact_bookings fb
  GROUP BY 1,2,3
),
merged AS (
  SELECT
    b.*,
    COALESCE(r.realized_rev,0) AS realized_rev,
    CASE WHEN b.successful_bookings > 0 THEN COALESCE(r.realized_rev,0)*1.0 / b.successful_bookings ELSE NULL END AS adr,
    CASE WHEN b.capacity > 0 THEN COALESCE(r.realized_rev,0)*1.0 / b.capacity ELSE NULL END AS revpar,
    CASE WHEN b.capacity > 0 THEN b.successful_bookings*1.0 / b.capacity ELSE NULL END AS occupancy
  FROM base b
  LEFT JOIN rev r
    ON r.property_id   = b.property_id
   AND r.check_in_date = b.check_in_date
   AND r.room_category = b.room_category
)
SELECT
  month_label,
  SUM(successful_bookings) AS rooms_sold,
  AVG(adr)    AS avg_adr,
  AVG(revpar) AS avg_revpar,
  AVG(occupancy) AS avg_occupancy
FROM merged
GROUP BY month_label
ORDER BY MIN(check_in_date);


-- GAP ONE (Extended)
-- Objective: Segment performance by city, booking platform, and weekday/weekend.
-- Insight: Highlights platform strengths and weaknesses across markets.


WITH base AS (
    SELECT
        fab.property_id,
        fab.check_in_date,
        fab.room_category,
        fab.successful_bookings,
        fab.capacity,
        dd.day_type,       -- Weekday / Weekend
        dh.city,
        fb.booking_platform
    FROM fact_aggregated_bookings fab
    JOIN dim_date dd
        ON fab.check_in_date = dd.date
    JOIN dim_hotels dh
        ON fab.property_id = dh.property_id
    JOIN fact_bookings fb
        ON fab.property_id = fb.property_id
       AND fab.check_in_date = fb.check_in_date
       AND fab.room_category = fb.room_category
),
rev AS (
    SELECT
        property_id,
        check_in_date,
        room_category,
        SUM(revenue_realized) AS realized_rev
    FROM fact_bookings
    GROUP BY property_id, check_in_date, room_category
),
merged AS (
    SELECT
        b.*,
        COALESCE(r.realized_rev, 0) AS realized_rev,
        CASE 
            WHEN b.capacity > 0 
            THEN b.successful_bookings::numeric / b.capacity 
            ELSE NULL 
        END AS occupancy,
        CASE 
            WHEN b.successful_bookings > 0 
            THEN COALESCE(r.realized_rev, 0)::numeric / b.successful_bookings 
            ELSE NULL 
        END AS adr,
        CASE 
            WHEN b.capacity > 0 
            THEN COALESCE(r.realized_rev, 0)::numeric / b.capacity 
            ELSE NULL 
        END AS revpar
    FROM base b
    LEFT JOIN rev r
        ON r.property_id = b.property_id
       AND r.check_in_date = b.check_in_date
       AND r.room_category = b.room_category
)
SELECT
    city,
    booking_platform,
    day_type,
    ROUND(AVG(occupancy) * 100, 2) AS avg_occupancy_percent,
    ROUND(AVG(adr), 2) AS avg_adr,
    ROUND(AVG(revpar), 2) AS avg_revpar
FROM merged
GROUP BY city, booking_platform, day_type
ORDER BY city, booking_platform, day_type;

-- GAP TWO (Extended)
-- Objective: Analyze booking lead times for weekdays vs weekends.
-- Insight: Understand booking behavior (last-minute vs planned) across city & platform.

WITH booking_data AS (
    SELECT
        dh.city,
        fb.booking_platform,
        dd.day_type,
        dd.date - fb.booking_date AS lead_time_days,
        CASE 
            WHEN LOWER(fb.booking_status) LIKE '%cancel%' THEN 1 
            ELSE 0 
        END AS is_cancel
    FROM fact_bookings fb
    JOIN dim_hotels dh 
        ON fb.property_id = dh.property_id
    JOIN dim_date dd 
        ON fb.check_in_date = dd.date
),
bucketed AS (
    SELECT
        city,
        booking_platform,
        day_type,
        CASE 
            WHEN lead_time_days < 3 THEN '0-2 days'
            WHEN lead_time_days BETWEEN 3 AND 7 THEN '3-7 days'
            WHEN lead_time_days BETWEEN 8 AND 14 THEN '8-14 days'
            WHEN lead_time_days BETWEEN 15 AND 30 THEN '15-30 days'
            ELSE '31+ days'
        END AS lead_time_bucket,
        is_cancel
    FROM booking_data
)
SELECT
    city,
    booking_platform,
    day_type,
    lead_time_bucket,
    COUNT(*) AS total_bookings,
    ROUND(AVG(is_cancel) * 100, 2) AS cancellation_rate_percent
FROM bucketed
GROUP BY city, booking_platform, day_type, lead_time_bucket
ORDER BY city, booking_platform, day_type, lead_time_bucket;


-- GAP THREE
-- Objective: Assess cancellations by room category × lead time.
-- Insight: Identify vulnerable room types (RT4) and how lead time impacts cancellations.

WITH booking_data AS (
    SELECT
        dh.city,
        fb.booking_platform,
        dd.day_type,
        fb.room_category,
        dd.date - fb.booking_date AS lead_time_days,
        CASE 
            WHEN LOWER(fb.booking_status) LIKE '%cancelled%' THEN 1 
            ELSE 0 
        END AS is_cancel
    FROM fact_bookings fb
    JOIN dim_hotels dh 
        ON fb.property_id = dh.property_id
    JOIN dim_date dd 
        ON fb.check_in_date = dd.date
    WHERE LOWER(fb.room_category) LIKE '%rt4%'
),
bucketed AS (
    SELECT
        city,
        booking_platform,
        day_type,
        CASE 
            WHEN lead_time_days < 3 THEN '0-2 days'
            WHEN lead_time_days BETWEEN 3 AND 7 THEN '3-7 days'
            WHEN lead_time_days BETWEEN 8 AND 14 THEN '8-14 days'
            WHEN lead_time_days BETWEEN 15 AND 30 THEN '15-30 days'
            ELSE '31+ days'
        END AS lead_time_bucket,
        is_cancel
    FROM booking_data
)
SELECT
    city,
    booking_platform,
    day_type,
    lead_time_bucket,
    COUNT(*) AS total_bookings,
    ROUND(AVG(is_cancel) * 100, 2) AS cancellation_rate_percent
FROM bucketed
GROUP BY city, booking_platform, day_type, lead_time_bucket
ORDER BY city, booking_platform, day_type, lead_time_bucket;


-- GAP THREE (Alternative View)
-- Objective: Another way to segment cancellations by room category × lead time.
-- Insight: Aggregates cancellations at a city × platform × day_type level.

WITH base AS (
    SELECT
        fb.booking_id,
        h.city,
        fb.booking_platform,
        dd.day_type,
        fb.booking_date,
        fb.check_in_date,
        fb.booking_status,
        CASE 
            WHEN fb.booking_status ILIKE '%cancel%' THEN 1 ELSE 0 
        END AS is_cancel,
        (fb.check_in_date - fb.booking_date) AS lead_time
    FROM fact_bookings fb
    JOIN dim_hotels h 
        ON fb.property_id = h.property_id
    JOIN dim_date dd 
        ON fb.check_in_date = dd.date
    WHERE fb.room_category LIKE '%RT4%'
),
bucketed AS (
    SELECT
        city,
        booking_platform,
        day_type,
        CASE
            WHEN lead_time < 0 THEN 'Invalid'
            WHEN lead_time BETWEEN 0 AND 3 THEN '0-3 days'
            WHEN lead_time BETWEEN 4 AND 7 THEN '4-7 days'
            WHEN lead_time BETWEEN 8 AND 14 THEN '8-14 days'
            WHEN lead_time BETWEEN 15 AND 30 THEN '15-30 days'
            ELSE '30+ days'
        END AS lead_time_bucket,
        COUNT(*) AS total_bookings,
        SUM(is_cancel) AS cancelled_bookings,
        ROUND(100.0 * SUM(is_cancel) / COUNT(*), 2) AS cancellation_rate
    FROM base
    GROUP BY city, booking_platform, day_type, lead_time_bucket
)
SELECT day_type,lead_time_bucket,SUM(total_bookings),SUM(cancelled_bookings),SUM(cancellation_rate)
FROM bucketed
GROUP BY day_type,lead_time_bucket
ORDER BY day_type, lead_time_bucket;



-- Booking Source Analysis
-- Objective: Compare OTA vs direct booking shares by room category.
-- Insight: Identifies dependency on OTAs for premium categories vs direct channels.

WITH base AS (
    SELECT
        fb.room_category,
        fb.booking_platform,
        COUNT(*) AS total_bookings
    FROM fact_bookings fb
    GROUP BY fb.room_category, fb.booking_platform
),
room_totals AS (
    SELECT
        room_category,
        SUM(total_bookings) AS room_total
    FROM base
    GROUP BY room_category
),
total AS (SELECT
    b.room_category,
    b.booking_platform,
    b.total_bookings,
    ROUND(100.0 * b.total_bookings / rt.room_total, 2) AS platform_share_percent
FROM base b
JOIN room_totals rt
    ON b.room_category = rt.room_category
ORDER BY b.room_category, platform_share_percent DESC)

SELECT room_category,
	SUM(CASE WHEN booking_platform NOT LIKE '%direct%' THEN total_bookings ELSE 0 END) AS OTA_total_bookings,
	SUM(CASE WHEN booking_platform LIKE '%direct%' THEN total_bookings ELSE 0 END) AS direct_total_bookings,
	SUM(CASE WHEN booking_platform NOT LIKE '%direct%' THEN platform_share_percent ELSE 0 END) AS OTA_share_percent,
	SUM(CASE WHEN booking_platform LIKE '%direct%' THEN platform_share_percent ELSE 0 END) AS direct_share_percent
FROM total
GROUP BY 1;


-- Projection 1
-- Objective: Estimate performance if successful bookings rise by 10%.
-- Impact: Shows projected occupancy, ADR, and RevPAR uplift (capped at capacity).

WITH base AS (
  SELECT
    fab.property_id,
    fab.check_in_date,
    fab.room_category,
    CASE WHEN (FLOOR(fab.successful_bookings*1.1)>capacity ) THEN capacity ELSE FLOOR(fab.successful_bookings*1.1) END AS successful_bookings,
    fab.capacity,
    dd.day_type,         -- 'Weekday' or 'Weekend'
    dd.mmm_yy
  FROM fact_aggregated_bookings fab
  JOIN dim_date dd
    ON fab.check_in_date = dd.date
),
-- bring in realized revenue per (property, date, room_category) from booking-level facts
rev AS (
  SELECT
    fb.property_id,
    fb.check_in_date,
    fb.room_category,
    SUM(fb.revenue_realized) AS realized_rev
  FROM fact_bookings fb
  -- keep all bookings; realized revenue already nets out cancels if your ETL does that
  GROUP BY 1,2,3
),
merged AS (
  SELECT
    b.*,
    COALESCE(1.1*r.realized_rev, 0) AS realized_rev,
    CASE WHEN b.capacity > 0 THEN b.successful_bookings*1.0 / b.capacity ELSE NULL END AS occupancy,
    CASE WHEN b.successful_bookings > 0 THEN COALESCE(1.1*r.realized_rev,0)*1.0 / b.successful_bookings ELSE NULL END AS adr,
CASE WHEN b.capacity > 0 THEN COALESCE(1.1*r.realized_rev,0)*1.0 / b.capacity ELSE NULL END AS revpar
  FROM base b
  LEFT JOIN rev r
    ON r.property_id = b.property_id
   AND r.check_in_date = b.check_in_date
   AND r.room_category = b.room_category
)
SELECT day_type,
  AVG(occupancy) AS avg_occupancy_projected,
  AVG(adr)       AS avg_adr_projected,
  AVG(revpar)    AS avg_revpar_projected
FROM merged
GROUP BY day_type
ORDER BY day_type;



-- Objective: Analyze city-wise hotel revenue and project a 10% growth scenario  
-- Outcome: Displays current revenue, projected revenue (with 10% growth),  
--          and the additional revenue needed per city, ranked by highest projection   


WITH city_current AS (
    SELECT 
        h.city,
        SUM(fb.revenue_realized) AS current_revenue
    FROM fact_bookings fb
    JOIN dim_hotels h ON fb.property_id = h.property_id
    GROUP BY h.city
)
SELECT 
    city,
    current_revenue,
    ROUND(current_revenue * 1.10, 2) AS projected_revenue,
    ROUND(current_revenue * 0.10, 2) AS additional_revenue
FROM city_current
ORDER BY projected_revenue DESC;
