import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
from database_utils import DatabaseConnector
from database_utils import get_rds_engine_object
import tabula
import boto3
from io import StringIO
import json
import requests
import time

class DataExtractor():
    def __init__(self):
        pass

    # Method to read a table from a relational database system (RDS) using an engine     
    def read_rds_table(self, table, engine):
        with engine.connect() as connection:
            self.dataframe = pd.read_sql_table(table, connection)
            return self.dataframe
    
    # Method to retrieve data from a PDF file
    def retrieve_pdf_data(self, pdf_path):
        dataframes = tabula.read_pdf(pdf_path, pages="all", stream=True)
        print(len(dataframes))
        return dataframes
    
    # Method to extract data from an S3 bucket and read it into a DataFrame
    def extract_from_s3(self, s3_address, local_file_path, user_profile_name):
        try:
            session = boto3.Session(profile_name=user_profile_name)
            s3 = session.client("s3")
            bucket, key = s3_address.replace("s3://", "").split("/", 1)
            s3.download_file(bucket, key, local_file_path)
            df = pd.read_csv(local_file_path)
            return df
        except Exception as e:
            print(f"Error extracting data from S3: {e}")
            return None
    
    # Method to extract JSON data from an S3 bucket and read it into a DataFrame
    def extract_json_from_s3(self, s3_address, local_file_path, user_profile_name):
        try:
            session = boto3.Session(profile_name=user_profile_name)
            s3 = session.client("s3")
            bucket, key = s3_address.replace("s3://", "").split("/", 1)
            s3.download_file(bucket, key, local_file_path)
            df = pd.read_json(local_file_path)
            return df
        except Exception as e:
            print(f"Error extracting data from S3: {e}")
            return None

# Function to extract data from an RDS table
def mrdc_rds_extract(table_name):
    mrdc_engine_object = get_rds_engine_object()
    mrdc_extract = DataExtractor()
    mrdc_extract.read_rds_table(table_name,mrdc_engine_object)
    table_name = mrdc_extract.dataframe
    return table_name

# Function to extract data from a PDF file
def extract_pdf(pdf_path):
    pdf_extract = DataExtractor()
    dataframes = pdf_extract.retrieve_pdf_data(pdf_path)
    if isinstance(dataframes, list):
        result_dataframe = pd.concat(dataframes, ignore_index=True)
    else:
        result_dataframe = dataframes
    return result_dataframe

# Function to extract a CSV file from an S3 bucket
def extract_csv_from_s3_bucket(s3_address, products_csv_path, user_profile_name):
    s3_extract = DataExtractor()
    dataframe = s3_extract.extract_from_s3(s3_address, products_csv_path, user_profile_name)
    return dataframe

# Function to extract a JSON file from an S3 bucket
def extract_json_from_s3_bucket(s3_address, products_csv_path, user_profile_name):
    s3_extract = DataExtractor()
    dataframe = s3_extract.extract_json_from_s3(s3_address, products_csv_path, user_profile_name)
    return dataframe

# Function to get store details from a REST API
def get_store_details(base_url, store_number):
    endpoint_url = f"{base_url}/store_details/{store_number}"
    response = requests.get(endpoint_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None

# Function to get details of all stores from a REST API
def get_all_store_details(base_url, min_store_number, max_store_number):
    all_stores_list = []
    requests_per_second = 3
    for store_number in range(min_store_number,max_store_number+1):
        all_stores_list.append(get_store_details(base_url, store_number))
        time.sleep(1 / requests_per_second)
    return all_stores_list

# Function to get the number of stores from a REST API
def get_number_of_stores(base_url):
    endpoint_url = f"{base_url}/number_stores"
    response = requests.get(endpoint_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None


api_key = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
headers = {"x-api-key": api_key}
base_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod"
products_csv_path = r"MRDC\products.csv"

if __name__ == "__main__":
    mrdc_engine_object = get_rds_engine_object()
    legacy_users = mrdc_rds_extract("legacy_users")
    card_data = extract_pdf("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
    api_key = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    headers = {"x-api-key": api_key}
    base_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod"
    products_csv_path = r"C:\Users\Dr Dankali\AICORE\MRDC\products.csv"
    extract_csv_from_s3_bucket("s3://data-handling-public/products.csv", products_csv_path, "AICORE1")
    get_store_details()
    get_number_of_stores("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod")
    s1 = get_store_details(1)
    type(s1)
    all_stores = get_all_store_details(0,450)
    orders_table = mrdc_rds_extract("orders_table")
    extract_json_from_s3_bucket()

