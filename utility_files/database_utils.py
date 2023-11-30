# %%
import yaml
import yaml
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect

# This class is used to connect to a database.
class DatabaseConnector():
    def __init__(self):
        pass

# Reads the database credentials from a YAML file.
# Parameters: cred_yaml_file (str): The path to the YAML file containing the database credentials.
# Returns: dict: A dictionary containing the database credentials
    def read_db_creds(self, cred_yaml_file):
        with open(cred_yaml_file, "r") as f:
            self.cred_dict = yaml.load(f, Loader = yaml.FullLoader)
            return self.cred_dict

# Initializes the database engine using the credentials read from the YAML file.
# Returns: sqlalchemy.engine.Engine: The SQLAlchemy engine object.   
    def init_db_engine(self):
        try:
            self.DATABASE_TYPE = 'postgresql'
            self.DBAPI = 'psycopg2'
            self.HOST = self.cred_dict["RDS_HOST"]
            self.USER = self.cred_dict["RDS_USER"]
            self.PASSWORD = self.cred_dict["RDS_PASSWORD"]
            self.DATABASE = self.cred_dict["RDS_DATABASE"]
            self.PORT = self.cred_dict["RDS_PORT"]
            self.engine = create_engine(f"{self.DATABASE_TYPE}+{self.DBAPI}://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}")
            self.engine.execution_options(isolation_level='AUTOCOMMIT').connect()
            print("Connection to the database was successful.")
            return self.engine
        except Exception as e:
            print(f"An error occurred while connecting to the database: {e}")

# Lists all the tables in the database.  
    def list_db_tables(self):
        try:
            self.engine.execution_options(isolation_level='AUTOCOMMIT').connect()
            inspector = inspect(self.engine)
            self.tables = inspector.get_table_names()
            print("Connection to the database was successful.")
            print(self.tables)
            
        except Exception as e:
            print(f"An error occurred while connecting to the database: {e}")

# Initializes the PostgreSQL database engine using the credentials read from the YAML file.    
    def init_postgresql_db_engine(self):
        self.db_username = self.cred_dict["username"]
        self.db_password = self.cred_dict["password"]
        self.db_host = self.cred_dict["host"]
        self.db_port = self.cred_dict["port"]
        self.db_name = self.cred_dict["name"]
        self.engine = create_engine(f'postgresql://{self.db_username}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')
        try: 
            self.engine.connect()
            print("Connection successful!")
            
            return self.engine
        except Exception as e:
            print(f"Exception: {e}")

# Uploads a dataframe to the database.
# Parameters:
# dataframe (pandas.DataFrame): The dataframe to be uploaded.
# new_table_name (str): The name of the new table in the database.
def upload_to_db(self, dataframe, new_table_name):
    dataframe.to_sql(new_table_name, self.engine, index=False, if_exists='replace')

# Gets the MRDC engine object.
# Returns: sqlalchemy.engine.Engine: The SQLAlchemy engine object.
def get_rds_engine_object():
    mrdc_connect = DatabaseConnector()
    mrdc_connect.read_db_creds(db_yaml_credentials)
    mrdc_connect.init_db_engine()
    mrdc_connect.list_db_tables()
    return mrdc_connect.engine

# Gets the sales data engine object.
# Returns: sqlalchemy.engine.Engine: The SQLAlchemy engine object.
def get_sales_data_engine_object():
    sd_connect = DatabaseConnector()
    sd_connect.read_db_creds(sales_data_yaml_credentials)
    sd_connect.init_postgresql_db_engine()
    return sd_connect.engine, sd_connect

# Uploads a dataframe to the database.
# Parameters:
# dataframe (pandas.DataFrame): The dataframe to be uploaded.
# new_table_name (str): The name of the new table in the database.
def upload_df_to_db(dataframe, new_table_name):
    sql_connect = DatabaseConnector()
    sql_connect.read_db_creds(sales_data_yaml_credentials)
    sql_connect.init_postgresql_db_engine()
    sql_connect.upload_to_db(dataframe, new_table_name)

db_yaml_credentials = "db_creds.yaml"
sales_data_yaml_credentials = "sales_data_yaml_credentials.yaml"

if __name__ == "__main__":
    get_sales_data_engine_object()
    get_rds_engine_object()
    upload_df_to_db()


