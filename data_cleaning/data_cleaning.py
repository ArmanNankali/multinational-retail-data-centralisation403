import pandas as pd
from dateutil import parser
from database_utils import upload_df_to_db
from data_extraction import mrdc_rds_extract, extract_pdf, extract_json_from_s3_bucket, extract_csv_from_s3_bucket, get_number_of_stores, get_all_store_details
import phonenumbers
import pycountry
import pycountry_convert as pc
import re
from datetime import datetime
from pint import UnitRegistry

class DataCleaning():
    def __init__(self, df):
        self.df = df
    
    def clean_user_data(self):
    
        self.to_YYYY_MM_DD("date_of_birth")
        self.to_YYYY_MM_DD("join_date")
        self.to_string_capital("first_name")
        self.to_string_capital("last_name")
        self.to_string("email_address")
        self.address_reformat("address")
        self.parse_and_correct_country_names("country")
        self.fill_country_codes_from_names("country", "country_code")
        self.format_phone_numbers("phone_number", "country_code")
        self.df.dropna(inplace=True)
        print("clean_user_data successful")
        
    def clean_card_data(self):
        self.df.drop("card_number expiry_date", inplace=True, axis=1)
        self.df.drop("Unnamed: 0", inplace=True, axis=1)
        self.reformat_from_MM_YY("expiry_date")
        self.to_YYYY_MM_DD("date_payment_confirmed")
        self.remove_non_integer("card_number")
        self.to_string("card_provider")
        self.filter_invalid_card_lengths("card_number", "card_provider")
        self.to_int64("card_number")

    def clean_stores_data(self):
        try:
                self.df = (
                    self.df
                    .drop("lat", axis=1)
                    .loc[self.df["store_type"].isin(["Web Portal", "Local", "Super Store", "Mall Kiosk", "Outlet"])]
                    )
                self.parse_and_correct_country_codes("country_code")
                self.fill_continent_from_country_codes("country_code", "continent")
                self.address_reformat("address")
                self.to_YYYY_MM_DD("opening_date")
                self.extract_numbers_from_column("staff_numbers")               
                print("clean_stores_data successful")
                return self.df
        except Exception as e:
            print("clean_stores_data unsuccessful")
            print(f"error : {e}")
        
    def clean_product_details(self):
        self.to_string_capital("product_name")
        self.remove_currency("product_price", "£")
        self.to_float64("product_price")
        self.to_category("category")
        self.to_int64("EAN")
        self.to_YYYY_MM_DD("date_added")
        self.to_string("uuid")
        self.to_category("removed")
        self.to_string("product_code")
        return self
    
    def clean_orders_table(self):
        try:
            self.df = (
                self.df
                .drop("first_name", axis=1)
                .drop("last_name", axis=1)
                .drop("1", axis=1)
                .drop("level_0", axis=1)
                )
            print("clean_orders_table successful")
        except Exception as e:
            print(f"clean_orders_table failed, error:{e}")
        return self.df   

    def clean_order_dates(self):
        try:
            self.df = (
                self.df
                .drop("month", axis=1)
                .drop("year", axis=1,)
                .drop("day", axis=1,)
            )
            self.to_YYYY_MM_DD("date")
            self.to_category("time_period")
            self.to_string("date_uuid")
            invalid_input = ["SAAZHF87TI"]
            self.remove_invalid_inputs("timestamp", invalid_input)
            self.drop_rows_isna("date")
            self.to_HH_MM_SS("timestamp")
            print("clean_order_dates successful")
        except Exception as e:
            print(f"clean_order_dates failed, error;{e}")
        return self.df

    def to_HH_MM_SS(self, colname):
        try:
            self.df[colname] = pd.to_datetime(self.df[colname])
            self.df[colname] = self.df[colname].apply(lambda x: x.strftime("%H:%M:%S"))
            
            print(self.df[colname].dtypes)
            print(f"{colname} sample value: {self.df[colname].iloc[0]}")
            print("To HH:MM:SS conversion successful")
        except Exception as e:
            print(e)
            print("To HH:MM:SS conversion unsuccessful")

    def drop_rows_isna(self, col):
        self.df = self.df[~self.df[col].isna()]
        return self.df
    
    def extract_numbers_from_column(self, column):
        try:
            def extract_numbers(text):
                numbers = re.findall(r'\d+', str(text))
                return int(numbers[0]) if numbers else None
        except Exception as e:
            print(f"Error: {e}")
    
        self.df.loc[::,column] = self.df[column].apply(extract_numbers)

    def remove_currency(self, column, currency):
        self.df[column] = self.df[column].str.replace(currency, '')

    def drop_column(self, colname):
        try:
            self.df = self.df.drop(colname, axis=1)
            print("drop_column successful")
            return self.df
        except Exception as e:
            print(f"drop_column failed error:{e}")
    
    def drop_rows_with_value(self, colname, value):
        try:
            self.df = self.df[self.df[colname] != value]
            print("drop_rows_with_value successful")
            return self.df
        except Exception as e:
            print(f"drop_rows_with_value failed error:{e}")

    def parse_and_correct_country_codes(self, country_code_col_name):
        def parse_country_code(code):
            try:
                country = pycountry.countries.get(alpha_2=code)
                return country.alpha_2 if country else "N/A"
            
            except LookupError:
                print("parse_country_code unsuccessfull")
                return "N/A"
            
        self.df[country_code_col_name] = self.df[country_code_col_name].apply(parse_country_code)
        print("parse_country_code successfull")
        print(self.df[country_code_col_name].value_counts())
        return self
    
   
    def fill_continent_from_country_codes(self, country_code_col_name, continent_col_name):
        def get_continent(country_code):
            try:
                continent_code = pc.country_alpha2_to_continent_code(country_code)
                continent_name = pc.convert_continent_code_to_continent_name(continent_code)
                return continent_name
            except Exception as e:
                print(f"fill_continent_from_country_codes failed, error:{e}")
                return "Unknown"

        self.df[continent_col_name] = self.df[country_code_col_name].apply(get_continent)
        print("fill_continent_from_country_codes complete")
        return self

    def remove_invalid_inputs(self, col, invalid_inputs):
        try:
            self.df = self.df[~self.df[col].isin(invalid_inputs)]
            print("remvoe_invalid_input successful")
        except Exception as e:
            print(f"remove_invalid_input failed, error:{e}")    

    def keep_only_valid_inputs(self, col, valid_inputs):
        self.df = self.df[self.df[col].isin(valid_inputs)]

    def address_reformat(self, address_col):
        try:
            def remove_n(address_with_newlines):
                address_with_commas = address_with_newlines.replace("\n", ", ")
                return address_with_commas
            self.df[address_col] = self.df[address_col].apply(remove_n)
            print("address_reformat successful")
            
        except Exception as e:
            print(f"address_reformat unsuccessful, error: {e}")
        return self
    
    def format_phone_numbers(self, phone_number_col, country_code_col):
        def parse_phone_number(row):
            try:
                parsed_number = phonenumbers.parse(row[phone_number_col], row[country_code_col])
                formatted_number = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
                return formatted_number
            except phonenumbers.NumberParseException as e:
                print(f"Error parsing phone number: {e}")
                return row[phone_number_col]
        print("format_phone_numbers complete")    
        self.df[phone_number_col] = self.df.apply(parse_phone_number, axis=1)
    
    def parse_and_correct_country_names(self, country_col_name):
        def parse_country_name(name):
            try:
                return pycountry.countries.get(name=name).name
            except Exception as e:
                print(f"parse_country_name failed for {name}, error: {e}")
                return "N/A"

        self.df[country_col_name] = self.df[country_col_name].apply(parse_country_name)
        print("parse_and_correct_country_names completed")
    
    def fill_country_codes_from_names(self, country_col_name, country_code_col_name):
        def get_country_code(name):
            try:
                return pycountry.countries.get(name=name).alpha_2
            except:
                return "N/A"

        self.df[country_code_col_name] = self.df[country_col_name].apply(get_country_code)
    
    def remove_non_integer(self, column_name):
        try:
            self.df[column_name] = self.df[column_name].astype(str).str.replace(r'\D', '', regex=True)
            self.df[column_name] = self.df[column_name].replace('', pd.NA)
            self.df[column_name] = pd.to_numeric(self.df[column_name], errors='coerce').astype("Int64")
            print("remove_non_integer successfull")
        except Exception as e:
            print(f"remove_non_integer failed error: {e}")

    def filter_invalid_card_lengths(self, card_number_col, card_provider_col):
        
        regex_patterns = {
            "VISA 16 digit": r"^4[0-9]{15}$",
            "JCB 16 digit": r"^(?:2131|1800|35\d{3})\d{11}$",
            "VISA 13 digit": r"^4[0-9]{12}(?:[0-9]{3})?$",
            "JCB 15 digit": r"^(?:2131|1800|35\d{3})\d{10}$",
            "VISA 19 digit": r"^4[0-9]{18}$",
            "Diners Club / Carte Blanche": r"^3(?:0[0-5]|[68][0-9])[0-9]{11}$",
            "American Express": r"^3[47][0-9]{13}$",
            "Maestro": r"^(?:5[0678]\d\d|6304|6390|67\d\d)\d{8,15}$",
            "Discover": r"^6(?:011|5[0-9]{2})[0-9]{12}$",
            "Mastercard": r"^5[1-5][0-9]{14}$",  
        }
        self.df[card_number_col] = self.df[card_number_col].astype(str)
        self.df[card_provider_col] = self.df[card_provider_col].astype(str)
        pattern_match = lambda row: regex_patterns.get(row[card_provider_col]) and re.match(regex_patterns[row[card_provider_col]], row[card_number_col])
        self.df = self.df[self.df.apply(pattern_match, axis=1).notna()]
    
    def check_store_code_format(self, store_code_column):
        self.df[store_code_column] = self.df[store_code_column].str.extract(r"^([A-Za-z]{2,3}-\w{8})$")
        self.df = self.df.dropna(subset=["store_code"])

    
    def data_type(self):
        return self.df.dtypes

    def describe(self):
        print(self.df.describe())

    def unique_categories(self):
        for col in self.df:
            unique_values = {}
            if self.df[col].dtypes.name == "category":  
                unique_values[col] = list(self.df[col].unique())
                print(f"{col} column has the following unique values {unique_values}")
                
    def data_shape(self):
        print(self.df.shape)
    
    def null_percent(self):
        for col in self.df:
            null_percents = {}
            try:
                null_percentage = ((self.df[col].isnull().sum()/len(self.df[col]))*100).round(2)
                null_percents[col] = null_percentage
                if null_percentage > 0:
                    print(f"{col} contains {null_percentage}% null values")
            except ZeroDivisionError:
                pass

    def to_float64(self, colname):
        self.colname = colname
        self.df[self.colname] = self.df[self.colname].astype("float64")
        print(self.df[self.colname].dtypes)
        print(f"{self.colname} sample value: {self.df[self.colname].iloc[0]}")

    
    def round_to_2(self, colname):
        self.colname = colname
        self.df[self.colname] = self.df[self.colname].round(2)
        print(self.df[self.colname].dtypes)
        print(f"{self.colname} sample value: {self.df[self.colname].iloc[0]}")
    
    def round_to_none(self, colname):
        self.colname = colname
        self.df[self.colname] = self.df[self.colname].round(0)
        print(self.df[self.colname].dtypes)
        print(f"{self.colname} sample value: {self.df[self.colname].iloc[0]}")

    def to_string(self, colname):
        try:
            self.colname = colname
            self.df.loc[:, self.colname] = self.df[self.colname].astype("str")
            print(self.df[self.colname].dtypes)
            print(f"{self.colname} sample value: {self.df[self.colname].iloc[0]}")
            print("to_string conversion successful")
        except Exception as e:
            print(e)
            print("to_string conversion unsuccessful")
    
    def to_string_capital(self, colname):
        try:
            self.colname = colname
            self.df[self.colname] = self.df[self.colname].astype("str").apply(lambda x: x.title())
            print(self.df[self.colname].dtypes)
            print(f"{self.colname} sample value: {self.df[self.colname].iloc[0]}")
            print("to_string_capital conversion successful")
        except Exception as e:
            print(e)
            print("to_string_capital conversion unsuccessful")
    
    def to_string_all_capitals(self, colname):
        try:
            self.colname = colname
            self.df[self.colname] = self.df[self.colname].astype("str").apply(lambda x: x.upper())
            print(self.df[self.colname].dtypes)
            print(f"{self.colname} sample value: {self.df[self.colname].iloc[0]}")
            print("to_string_all_capitals conversion successful")
        except Exception as e:
            print(e)
            print("to_string_all_capitals conversion unsuccessful")
        
    
    def to_int64(self, colname):
        self.colname = colname
        try:
            self.df.loc[:, self.colname] = self.df[self.colname].astype("int64")
            print(self.df[self.colname].dtypes)
            print(f"{self.colname} sample value: {self.df[self.colname].iloc[0]}")
            print("to_int64 successfull")
        except Exception as e:
            print(f"Error: {e}")
            print("to_int64 unsuccessfull")
    
    def percentage(self, colname):
        self.colname = colname
        self.df[self.colname] = self.df[self.colname].map('{:2%}'.format)
        print(self.df[self.colname].dtypes)
        print(f"{self.colname} sample value: {self.df[self.colname].iloc[0]}")
    
    def to_category(self, colname):
        self.colname = colname
        self.df[self.colname] = self.df[self.colname].astype("category")
        print(self.df[self.colname].dtypes)
        print(f"{self.colname} sample value: {self.df[self.colname].iloc[0]}")
    
    def parse_and_format_date(self, date_string):
        if isinstance(date_string, str):
            try:
                parsed_date = parser.parse(date_string)
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                return None
        else:
            try:
                parsed_date = parser.parse(str(date_string))
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                return None
    
    def parse_and_format_date_YYYY_MM(self, date_string):
        try:
            parsed_date = parser.parse(str(date_string))
            return parsed_date.strftime('%Y-%m')
        except Exception as e:
            print(f"parsing failed, error:{e} ")
            return None

    def to_YYYY_MM_DD(self, colname):
        try:
            self.df.loc[:, colname] = self.df[colname].apply(self.parse_and_format_date)
            print(self.df[colname].dtypes)
            print(f"{colname} sample value: {self.df[colname].iloc[0]}")
            print("To YYYY_MM_DD conversion successful")
            return self
        except Exception as e:
            print(e)
            print("To YYYY_MM_DD conversion unsuccessful")
        return self.df
    
    
    def reformat_from_MM_YY(self, colname):
        def convert_date_format(date_str):
            try:
                parsed_date = datetime.strptime(str(date_str), "%m/%y")
                new_format = parsed_date.strftime('%Y-%m')

                return new_format
            except Exception as e:
                print(f"reformat_from_MM_YY failed, error: {e}")
                return None
        try:
            self.df[colname] = self.df[colname].apply(convert_date_format)
            print(self.df[colname].dtypes)
            print(f"{colname} sample value: {self.df[colname].iloc[0]}")
            print("To YYYY_MM_DD conversion successful")
            return self
        except Exception as e:
            print(e)
            print("To YYYY_MM_DD conversion unsuccessful")
        return self.df
        

    def to_YYYY_MM(self, colname):
        self.colname = colname
        try:
            self.df[colname] = self.df[colname].apply(self.parse_and_format_date_YYYY_MM)
            print(self.df[self.colname].dtypes)
            print(f"{self.colname} sample value: {self.df[self.colname].iloc[0]}")
        except Exception as e:
            print(f"to_YYYY_MM failed, error: {e}")

def longest_character_check(df, column):
    length = 0
    for value in df[column]:
        if len(str(value)) > length:
            length =  len(str(value))
    return length

''' USER DATA CLEANING--------------------------------------------------'''
legacy_users = mrdc_rds_extract("legacy_users")
legacy_users.info()

''' ALTERATIONS:
 ISO YYYY-MM-DD formatting: date_of_birth, join_date
 Capitalise first character of string: first_name, last_name
 To string: email_address
 Remove \n from address: address             
 Parse for valid country names: country
 Use valid country names to fill country codes: country_code
 Use country codes to parse correct phone number format: phone_number
 Drop all null values
'''

lg1 = DataCleaning(legacy_users)
lg1.clean_user_data()
legacy_users.set_index("index")
legacy_users.info()

''' CLeaned legacy_users uploaded to SQL database '''
upload_df_to_db(legacy_users, "dim_users")

''' Longest country_code printed to inform later VARCHAR(?) formatting '''
print("longest country_code:")
print(longest_character_check(legacy_users, "country_code"))
''' COUNTRY_CODE-----------------------------------------------------'''
 
card_data = extract_pdf("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

''' Drop unnecessary columns: card_number expiry_date and Unnamed : 0
 Format dates to ISO MM_YY format: expiry_date
 Format dates to ISO YYYY_MM_DD format: date_payment_confirmed
 Remove non-ineteger values: card_number
 To string format: card_provider
 Checking card number regex format against those dictated by card provider
 To int64: card_number
'''
cd1 = DataCleaning(card_data)
cd1.clean_card_data()
card_data.dropna(axis = 0, inplace = True)
upload_df_to_db(card_data, "dim_card_details")

''' STORES_DATA-----------------------------------------------------'''
base_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod"
get_number_of_stores(base_url)
''' There are 451 stores'''

all_stores_list = get_all_store_details(base_url, 0,450)
''' The list of dictionaries is converted to a dataframe'''
stores_data = pd.DataFrame(all_stores_list)


import pickle
with open(r"C:\Users\Dr Dankali\AICORE\MRDC\stores_data.pkl", "rb") as file:
    stores_data = pickle.load(file)
'''
 Drop unneseccary column: "lat"
 Include only rows with valid store_type
 Parse and only include valid country codes
 Use country_code to determine and replace with valid continent
 Remove "\n" from addresses and replace with ","
 Convert opening_date to ISO format YYYY_MM_DD
 Exctract only integers from staff_numbers to remove the value J78
 '''
sd1 = DataCleaning(stores_data)
stores_data = sd1.clean_stores_data()

''' Convert the longitude value for the Web Portal store to null for SQL INT covnersion later'''
stores_data.loc[stores_data["store_type"]=="Web Portal", "longitude"] = None

''' The longest country_code is needed for later VARCHAR(?) formatting in SQL'''
print("longest country_code:")
print(longest_character_check(stores_data, "country_code"))

stores_data.loc[stores_data["store_type"]=="Web Portal", "longitude"] = None

''' Upload cleaned store details to the database'''
upload_df_to_db(stores_data, "dim_store_details")


'''PRODUCT DETAILS---------------------------------------------------------------------------'''
from data_extraction import DataExtractor, extract_csv_from_s3_bucket
product_details = extract_csv_from_s3_bucket("s3://data-handling-public/products.csv", r"C:\Users\Dr Dankali\AICORE\MRDC\products.csv", "AICORE1")

''' Here a new column "unit" is crated by looking for a regex pattern matching letters to denote unit'''
product_details['unit'] = product_details['weight'].str.extract(r'([a-zA-Z]+)')

''' This function converts all "x" unit weights to the quantity multiplied by the weight of a single unit'''
def multiply_weights(df, weight_col, unit_col):
    def parse_weight(row):
        if row[unit_col] == "x":
            numbers = re.findall(r"\d+", row[weight_col])
            if len(numbers) == 2:
                result = int(numbers[0]) * int(numbers[1])
            else:
                result = int(numbers[0])
            return f"{result}g"
        else:
            return row[weight_col]

    df[weight_col] = df.apply(parse_weight, axis=1)

multiply_weights(product_details, "weight", "unit")

''' This function converts all rows weights with a unit of "ml" to grams based on a roughly 1:1 ratio'''
def convert_ml_to_g(df, weight_col, unit_col):
    def parse_weight(row):
        if row[unit_col] == "ml":
            numbers = re.findall(r"\d+", row[weight_col])
            if len(numbers) == 2:
                result = int(numbers[0]) * int(numbers[1])
            else:
                result = int(numbers[0])
            return f"{result}g"
        else:
            return row[weight_col]

    df.loc[:, weight_col] = df.apply(parse_weight, axis=1)

convert_ml_to_g(product_details, "weight", "unit")

''' Here we remove all rows with invalid units, and include x as these rows still have that unit despite the weights being converted.'''
product_details = product_details[product_details["unit"].isin(["kg", "g", "ml", "oz", "x"])]

''' This fucntion converts all weights to kilograms by parsing them with the UnitRegistry module'''
def convert_weight_column_to_kg(df, weight_column):
    ureg = UnitRegistry()
    def convert_to_kg(row):
        weight = ureg(row)
        weight_kg = weight.to(ureg.kg)
        return weight_kg.magnitude
    df.loc[:,weight_column] = df[weight_column].apply(convert_to_kg)

convert_weight_column_to_kg(product_details, "weight")

''' Now we drop unnecessary columns'''
product_details.drop("Unnamed: 0", axis=1, inplace=True)
product_details.drop("unit", axis=1, inplace=True)
product_details.info()

''' All values are in "£" currency, so nothing needs to be done'''
product_details["product_price"] = product_details["product_price"].astype(str)
product_details["currency"] = product_details["product_price"].str.extract(r'(["£", "$", "€"]+)')
print(product_details["currency"].value_counts())
product_details.drop("currency", axis=1, inplace=True)
'''
 product_name first characters are capitalised
 The currency symbol is removed as they are all in "£"
 product_price is now a float64
 category is now a category
 EAN is now int64
 date_added formatted to ISO YYYY_MM_DD format
 uuid is now a string
 removed is now a category
 product_code is now a string 
 '''
pd1 = DataCleaning(product_details)
pd1.clean_product_details()

''' Cleaned product_details uploaded to sales datat SQL database'''
upload_df_to_db(product_details, "dim_products")

''' Longest EAN printed for later VARCHAR(?) formatting in SQL'''
print("longest EAN:")
print(longest_character_check(product_details, "EAN"))
''' ORDERS TABLE---------------------------------------------------------------------------'''
orders_table = mrdc_rds_extract("orders_table")
orders_table.info()

''' Drop columns with mostly nulls: first_name, last_name, 1, level_0'''
ot1 = DataCleaning(orders_table)
orders_table = ot1.clean_orders_table()
orders_table.info()

''' cleaned orders_table uploaded to SQL database'''
upload_df_to_db(orders_table, "orders_table")


''' ORDER DATES---------------------------------------------------------------------------'''

''' This function splits the full s3 link into bukcet and key, returning a reformatted s3 bucket path'''
def convert_url_to_s3_path(url):
    s3_path = url.replace('https://', '')
    bucket, key = s3_path.split('/', 1)
    bucket = bucket.split(".", 1)[0]
    s3_path = f"s3://{bucket}/{key}"
    return s3_path
order_date_url = convert_url_to_s3_path("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")
print(order_date_url)

''' JSON dataframe extracted'''
order_dates_df = extract_json_from_s3_bucket("s3://data-handling-public/date_details.json", r"order_dates.json", "AICORE1")
''' JSON read to pandas dataframe'''
order_dates = pd.read_json("order_dates.json")

''' The three month, day and year columns will be combined into a single date column'''
''' combining dates columns '''
def combine_row_date_info(df, *date_columns):
    df["date"] = df.apply(lambda row: '-'.join(str(row[col]) for col in date_columns), axis=1)
    
combine_row_date_info(order_dates, "year", "month", "day")

'''
 Redundant columns dropped: month, day, year
 date converted to ISO YYYY_MM_DD format
 time_period converted to catgeory
 date_uuid converted to string
 invalid timestamp row removed
 null date columns dropped
 timestamp converted to HH_MM_SS time format
 '''

od1 = DataCleaning(order_dates)
order_dates = od1.clean_order_dates()
order_dates.info()

''' cleaned order_dates uploaded to the SQL database'''
upload_df_to_db(order_dates, "dim_date_times")

''' Longest time_period printed to inform VARCHAR(?) later in SQL'''
print("longest time_period:")
print(longest_character_check(order_dates, "time_period"))