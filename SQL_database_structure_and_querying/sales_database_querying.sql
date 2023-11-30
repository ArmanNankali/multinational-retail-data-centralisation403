-- This query returns the number of stores for each country code
SELECT country_code, COUNT(*) as store_count 
FROM dim_store_details
GROUP BY country_code
ORDER BY store_count DESC; 

-- This query returns the number of stores for each locality
SELECT locality, COUNT(*) as total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC;

-- This query calculates the total sales for each month by multiplying the product quantity by the product price for each order
SELECT EXTRACT(MONTH FROM dt.date) as month, SUM(od.product_quantity * p.product_price) as total_sales
FROM orders_table od
JOIN dim_date_times dt ON od.date_uuid = dt.date_uuid
JOIN dim_products p ON od.product_code = p.product_code
GROUP BY month
ORDER BY total_sales DESC;

-- This query counts the number of sales and the total product quantity for each location. If the store type is ‘Web Portal’, the location is considered ‘Web’, otherwise it’s ‘Offline’
SELECT 
    COUNT(*) as numbers_of_sales,
    SUM(od.product_quantity) as product_quantity_count,
    CASE
        WHEN sd.store_type ='Web Portal' THEN 'Web'
        ELSE 'Offline'
    END as location
FROM orders_table od
JOIN dim_store_details sd ON od.store_code = sd.store_code
GROUP BY location;

-- This query calculates the total sales and the percentage of total sales for each store type
SELECT 
    sd.store_type as store_type,
    SUM(od.product_quantity * p.product_price) as total_sales,
    SUM(od.product_quantity * p.product_price) / SUM(SUM(od.product_quantity * p.product_price)) OVER () * 100 as percentage_total
FROM orders_table od
JOIN dim_products p ON od.product_code = p.product_code
JOIN dim_store_details sd ON od.store_code = sd.store_code
GROUP BY sd.store_type
ORDER BY percentage_total DESC;

-- This query first calculates the total sales for each month of each year. It then ranks these sales within each year. The final query returns the month, year, and total sales for the month with the highest sales in each year
WITH monthly_sales AS (
    SELECT 
        EXTRACT(MONTH FROM dt.date) as month,
        EXTRACT(YEAR FROM dt.date) as year,
        SUM(od.product_quantity * p.product_price) as total_sales
    FROM orders_table od
    JOIN dim_date_times dt ON od.date_uuid = dt.date_uuid
    JOIN dim_products p ON od.product_code = p.product_code
    GROUP BY month, year
),
ranked_sales AS (
    SELECT 
        month, 
        year, 
        total_sales,
        ROW_NUMBER() OVER (PARTITION BY year ORDER BY total_sales DESC) as sales_rank
    FROM monthly_sales
)
SELECT month, year, total_sales
FROM ranked_sales
WHERE sales_rank = 1
ORDER BY total_sales DESC;

-- This query sums the staff numbers for each country code
SELECT 
    SUM(staff_numbers) as total_staff_numbers,
    country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

-- This query calculates the total sales for each store type in Germany
SELECT 
    SUM(od.product_quantity * p.product_price) as total_sales,
    sd.store_type as store_type,    
    sd.country_code as country_code
FROM orders_table od
JOIN dim_products p ON od.product_code = p.product_code
JOIN dim_store_details sd ON od.store_code = sd.store_code
WHERE sd.country_code = 'DE'
GROUP BY sd.store_type, sd.country_code
ORDER BY total_sales;

-- This query first creates a timestamp for each row in the dim_date_times table by concatenating the date and time.
-- It then calculates the time difference between consecutive timestamps and averages these differences by year.
-- The final query extracts the hours, minutes, and seconds from these average time differences
WITH time_stamps AS(
    SELECT 
        (date::date || ' ' || timestamp::time)::timestamp as full_timestamp,
        EXTRACT(YEAR FROM date) as year
    FROM dim_date_times
),

all_time_difference AS (
    SELECT 
        EXTRACT(EPOCH FROM full_timestamp) - LAG(EXTRACT(EPOCH FROM full_timestamp)) OVER (ORDER BY full_timestamp) as time_difference,
        year
    FROM time_stamps
),

total_time_differences AS (
    SELECT
        AVG(time_difference) as average_time_difference,
        year
    FROM all_time_difference
    GROUP BY year
)

SELECT 
    EXTRACT(HOUR FROM average_time_difference * INTERVAL '1 second') as hours,
    EXTRACT(MINUTE FROM average_time_difference * INTERVAL '1 second') as minutes,
    EXTRACT(SECOND FROM average_time_difference * INTERVAL '1 second') as seconds,
    year
FROM total_time_differences
ORDER BY average_time_difference DESC; 
