-- Altering the orders_table to change the data types of columns
ALTER TABLE orders_table
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(11);

-- Altering the dim_users table to change the data types of columns
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
ALTER COLUMN join_date TYPE  DATE USING join_date::DATE,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid;

-- Altering the dim_store_details table to change the data types of columns
ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN continent TYPE VARCHAR(255),
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,
ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT;

-- Adding weight_class column to the dim_products table and updating its values based on the weight column
ALTER TABLE dim_products
ADD weight_class VARCHAR(14);

-- Updating the weight_class column's values based on the weight column
UPDATE dim_products
SET weight_class =
CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
END;

-- Renaming removed column in the dim_products table to still_available and updating its values
ALTER TABLE dim_products RENAME COLUMN removed TO still_available;

UPDATE dim_products
SET still_available =
CASE
    WHEN still_available = 'Still_avaliable' THEN TRUE
    WHEN still_available = 'Removed' THEN FALSE
END;

-- Altering the dim_products table to change the data types of columns
ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE VARCHAR(18),
ALTER COLUMN product_code TYPE VARCHAR(12),
ALTER COLUMN weight_class TYPE VARCHAR(14),
ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
ALTER COLUMN still_available TYPE BOOL USING still_available::BOOL,
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT;

-- Altering the dim_date_times table to change the data types of several columns
ALTER TABLE dim_date_times
ALTER COLUMN time_period TYPE VARCHAR(10),
ALTER COLUMN date TYPE DATE USING date::DATE,
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;

-- Altering the dim_card_details table to change the data types of several columns
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(20),
ALTER COLUMN expiry_date TYPE VARCHAR(8),
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

-- Adding primary keys
ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

-- Adding foreign keys to the orders_table
ALTER TABLE orders_table
ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid),
ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code),
ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid),
ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code);

-- This primary key was not possible because not all card numbers form dim_card_details are present in orders_table
-- ADD FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number)
