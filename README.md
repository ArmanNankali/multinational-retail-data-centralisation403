# multinational-retail-data-centralisation403
armannankali/multinational-retail-data-centralisation403

## Table of Contents, if the README file is long
Table of Contents
Project Description
Installation Instructions
Usage Instructions
File Structure
License Information

## Project description
This project includes a Python script, `database_utils.py`, which contains a class DatabaseConnector and several functions for interacting with a PostgreSQL database. The DatabaseConnector class is used to establish a connection with the original database, list all tables in the database and upload a dataframe to the new sales database. The functions get_rds_engine_object, get_sales_data_engine_object are used to get the database engine object and  upload_df_to_db to upload a dataframe to the database.

The `data_extraction.py` script contains a class DataExtractor and several functions for extracting  data from an RDS table, a PDF file, a CSV file from an S3 bucket or a JSON file from an S3 bucket into a DataFrame.

The `data_cleaning.py` script handles the cleaning of every dataset extracted. This involves converting columns to the correct type, capitalising the right part of strings and parsing each entry to convert and format ISO date , regional phone numbers, credit card length dictated by provider. Erroneus values and columns are also dropped if they can't be inferred.

The `sales_database_schema_creation.sql` file contains SQL commands to alter the structure of several tables in a database. The alterations include changing the data types of columns, adding new columns, renaming columns, and updating column values. Primary keys to the tables and foreign keys to the orders_table are also added.

The `sales_database_querying.sql` script explores the database to obtain up-to-date metrics of the performance of the business and each store across different geographical locations and store types.

## Installation instructions
This project requires Python 3.6 or later. The following Python packages are also required:

- yaml
- pandas
- sqlalchemy
- tabula
- boto3
- json
- requests
- time
- dateutil
- phonenumbers
- pycountry
- pycountry_convert
- re
- datetime
- pint

## Usage instructions
Clone this repository to your local machine.
Use the .py scripts in the order they are listed.
Remember to alter the file paths for the location of your own directory and relevant files
## File structure of the project
1. `database_utils.py`: DatabaseConnector: A class for connecting to a PostgreSQL database and uploading to another.
2. `data_extractor.py`: DataExtraction: This class and several functions extract data from various sources and convert them into a dataframe (RDS, CSV, JSON, S3)
3. `data_cleaning.py`: DataCleaning: This class and several other functions clean the data by: removing erroneus values, converting column data types, parsing and formatting dates, phone numbers and card numbers.
4. `sales_database_schema_creation.sql`: Here the table column types are altered and priamry and foerign key constraints are added.
5. `sales_database_querying.sql`: This script explores performance of the business and each store across different geographical locations and store types.
## License information
MIT License
