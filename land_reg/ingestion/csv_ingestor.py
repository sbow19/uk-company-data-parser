import re
import pandas as pd
from database.db_methods import execute_queries, fetch_address_ids, fetch_company_ids
from land_reg.utils.query_helpers import uk_parse_statements, overseas_parse_statements
from database.db_methods import sanitize_item
from config import config
import uuid
from datetime import datetime
import numpy as np
import logging
import sys

# Logging - refactor later

logging.basicConfig(
    filename='sqlalchemy_errors.log',  # Log file name
    filemode='a',   
    level=logging.ERROR,  # Change to DEBUG for more detailed output
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('sqlalchemy')

# Get ids associated with addresses from database
def _assign_address_ids(df):
    
    # Dictionary to hold address ids
    address_dict = fetch_address_ids()
        
    # Address ids (one per address column)
    for index, row in df.iterrows():

        property_address = row['Property Address']

        addresses_from_dataset = [(property_address, "property_address_id")]

        for i in range(1, 5):  # Loop through proprietors 1 to 4
            for j in range(1, 4):  # Loop through address lines 1 to 3
                address_key = f"Proprietor ({i}) Address ({j})"
                address_id = f"proprietor_{i}_address_{j}_id"
                addresses_from_dataset.append((row[address_key], address_id))
        
        for address in addresses_from_dataset:
            
            # Want to make sure any blank title addresses are still assigned an id, except for proprietor addresses
            if address[0] == "NAN" and address[1] != "property_address_id":
                df.iloc[index, df.columns.get_loc(address[1])] = "NAN"
            
            # If address name appears in list, then we assign the corresponding id to the address in the dataset
            if address[0] in address_dict:
                
                df.iloc[index, df.columns.get_loc(address[1])] = address_dict[address[0]]     # Assign address id to dataset
                continue
            else:
                # Get the current datetime
                now = datetime.now()

                # Convert to timestamp and then to int
                current_timestamp_int = int(now.timestamp())
                new_address_id = str(uuid.uuid4().int) + str(current_timestamp_int)
                df.iloc[index, df.columns.get_loc(address[1])] = new_address_id
                address_dict[address[0]] = new_address_id
    
        

def _assign_proprietor_ids(df, parse_type):
    # Dictionary to hold proprietor ids
    company_dict = fetch_company_ids()
    
    for index, row in df.iterrows():
        
        proprietors_from_dataset = []

        for i in range(1, 5):  # Loop through proprietors 1 to 4
            proprietor_name = row[f'Proprietor Name ({i})']
            proprietor_number = row[f'Company Registration No. ({i})']
            proprietor_country = row[f'Country Incorporated ({i})'] if parse_type == "overseas" else "UNITED KINGDOM"
            
            proprietors_from_dataset.append((proprietor_name, proprietor_number, proprietor_country, f"proprietor_{i}_id"))
        
        
        for proprietor in proprietors_from_dataset:
            
            # Skip if no name
            if proprietor[0] == 'NAN': 
                df.iloc[index, df.columns.get_loc(proprietor[3])] = "NAN"
                continue
            
            # If company name not in database
            if not proprietor[0] in company_dict["names_dict"]:
                # Shouldnt fire at all
                print(proprietor)
                # Get the current datetime
                now = datetime.now()
                # Convert to timestamp and then to int
                current_timestamp_int = int(now.timestamp())
                new_company_id = str(uuid.uuid4().int) + str(current_timestamp_int)
                
                df.iloc[index, df.columns.get_loc(proprietor[3])] = new_company_id
                company_dict["details"][new_company_id] = {
                    "id": new_company_id,
                    "number": proprietor[1],
                    "country_incorporated": proprietor[2],
                    "name": proprietor[0],
                }

                company_dict["names_dict"][proprietor[0]] = set()
                company_dict["names_dict"][proprietor[0]].add(new_company_id)  
        
            # Check if company name already exists in database, or already iterated over
            elif proprietor[0] in company_dict["names_dict"]:
                
                # get list of company ids associated with a company name and country  incorporated pair
                set_of_company_ids = company_dict["names_dict"][proprietor[0]]
                
                # Get company (or list of companies) from dictionary
                company_details_list = [company_dict["details"][company_id] for company_id in set_of_company_ids]
                
                # Flag variable
                match_found = False
                
                # Loop through to check whether company id already for company name/ country incorporated pair
                for company_details_dict in company_details_list:
                    # Check if there is a match between name, number and country incorporated
                    if  proprietor[1] == company_details_dict["number"] and proprietor[2] == company_details_dict["country_incorporated"]:
                        
                        df.iloc[index, df.columns.get_loc(proprietor[3])] = company_details_dict["id"] 
                        match_found = True
                        break
                
                if not match_found:
                    #Shouldnt fire at all
                    print(proprietor)
                    # Get the current datetime
                    now = datetime.now()
                    # Convert to timestamp and then to int
                    current_timestamp_int = int(now.timestamp())
                    new_company_id = str(uuid.uuid4().int) + str(current_timestamp_int)
                    
                    df.iloc[index, df.columns.get_loc(proprietor[3])] = new_company_id
                    company_dict["details"][new_company_id] = {
                        "id": new_company_id,
                        "number": proprietor[1],
                        "country_incorporated": proprietor[2],
                        "name": proprietor[0],
                    }
                    # Add id to set of ids associated with company name
                    company_dict["names_dict"][proprietor[0]].add(new_company_id)   
                    
                    
# Define the conversion function
def _convert_dates(date_str):
    date_str = str(date_str)
    
    if date_str == "nan":
        date_str = "01-01-1900" # Default string
        date_obj = datetime.strptime(date_str, "%d-%m-%Y")
        return date_obj.strftime("%Y-%m-%d")
    try:
        # Convert date from dd-mm-yyyy to yyyy-mm-dd
        date_obj = datetime.strptime(date_str, "%d-%m-%Y")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        return None  # Handle invalid date formats
    
def _convert_address_strings(string: str):
    
    # If null value exists, we return an address identifier
    if str(string) == "nan" or string == None:
        return string
    
    try:    
        string = string.replace("'", "''")
        string = string.replace("`", " ")
        string = string.replace(".", " ")
        string = string.replace('"', '""')
        string = string.replace("£", "GBP")
        string = string.replace(":", " ")
        string = string.replace("%", "%%")
        string = string.replace("True", "1")
        string = string.replace("False", "0")
        string = string.replace("\\", "\\\\")
        
        if len(string) > 699:
            return string[:700].upper().strip()
        else:
            return string.upper().strip()
    except Exception as e:
        raise Exception(f"Error parsing address string {e}")  # Handle invalid date formats
 
def _convert_address_strings_main(string: str):
    
    # If null value exists, we return an address identifier
    if str(string) == "nan" or string == None:
        now = datetime.now()
        return str(uuid.uuid4().int) + str(int(now.timestamp()))
    
    try:    
        string = string.replace("'", "''")
        string = string.replace("`", " ")
        string = string.replace(".", " ")
        string = string.replace('"', '""')
        string = string.replace("£", "GBP")
        string = string.replace(":", " ")
        string = string.replace("%", "%%")
        string = string.replace("True", "1")
        string = string.replace("False", "0")
        string = string.replace("\\", "\\\\")
        
        if len(string) > 699:
            return string[:700].upper().strip()
        else:
            return string.upper().strip()
    except Exception as e:
        raise Exception(f"Error parsing address string {e}")  # Handle invalid date formats
       

def _csv_parse_helper(file, parse_type, date_str):
    # Read through csv file in batches  
    batch_size = 10 ** 5
    row_count = 0
    
    df = pd.read_csv(file, encoding='utf-8')
    
    # Drop the last row
    df = df[:-1]

    # Append new column with date string
    df["dataset_date"] = date_str
    
    # Change date format of Date Proprietor Added
    df["Date Proprietor Added"] = df["Date Proprietor Added"].map(_convert_dates)
    
    # Standardise fonts for address columns
    df['Property Address'] =   df['Property Address'].map(_convert_address_strings_main)
    
    # Assign ids to unique data points (addresses, company names)
    df["property_address_id"] = np.nan

    for i in range(1, 5):
        df[f"Proprietor Name ({i})"] =  df[f"Proprietor Name ({i})"].map(sanitize_item)
        df[f"Country Incorporated ({i})"] =  df[f"Country Incorporated ({i})"].map(sanitize_item)
        df[f"Company Registration No. ({i})"] =  df[f"Company Registration No. ({i})"].map(sanitize_item) 
        df[f"proprietor_{i}_id"] = np.nan
        
        for j in range(1, 4):
            df[f"Proprietor ({i}) Address ({j})"] =  df[f"Proprietor ({i}) Address ({j})"].map(_convert_address_strings) 
            df[f"proprietor_{i}_address_{j}_id"] = np.nan
             
    # Set all columns to strings
    df = df.astype(str)
    
    # Assign address id / mutate df
    _assign_address_ids(df)
    
    _assign_proprietor_ids(df, parse_type)
    
    # Determine which statements should be created
    if parse_type == "uk":
        statement_builder = uk_parse_statements
    elif parse_type == "overseas":
        statement_builder = overseas_parse_statements
        
    while row_count < len(df):
        try:
            chunk = df[row_count: row_count + batch_size]
            statement_list = statement_builder(chunk)
            execute_queries(statement_list)
            row_count += batch_size
            
        except Exception as e:
            logger.error(str(e))
            raise Exception(f"Error processing at row {row_count}: {str(e)[:2000]}")
    

def get_date(filePath):

    #Date pattern in regex
    pattern = r'(\d{4})_(\d{2})'

    # Search for the pattern in the file path
    match = re.search(pattern, filePath)
    if match:
        year, month = match.groups()
        # Format the date as YYYY-MM-DD
        date_str = f"{year}-{month}-01"  # Assuming the day is the 1st of the month
        return date_str
    else:
        raise ValueError("Date not found in the file path")

def parse_overseas_records(filePath):

    try:
        #GEt file date string 
        date_str = get_date(filePath)

        # Open and read the CSV file
        _csv_parse_helper(filePath, "overseas", date_str)

        # Log the successfully parsed file path to the .txt file
        with open(config.PARSED_FILE_PATH, mode='a') as log:
            log.write(f"{filePath}\n")

        print(f"Successfully ingested {filePath}")
    
    except Exception as e:
        # Handle any exceptions that may occur
        print(f"Failed to ingest {filePath}: {str(e)[:100]}, Finished at {str(e)[:1000]}")
    


def parse_uk_records(filePath):
        
    try:
        #Get file date string from file name
        date_str = get_date(filePath)

        # Open and read the CSV file
        
        _csv_parse_helper(filePath, "uk", date_str)
            
        
        # Log the successfully parsed file path to the .txt file
        with open(config.PARSED_FILE_PATH, mode='a') as log:
            log.write(f"{filePath}\n")

        print(f"Successfully ingested {filePath}")
    
    except Exception as e:
        # Handle any exceptions that may occur
        print(f"Failed to ingest {filePath}: {e}, Finished at {e}")

        
# Function to loop over unparsed csvs.
def ingest_csv_files(filePathArray):

    for filePath in filePathArray:

        filePathString = str(filePath)
        
        #Check whether we are parsing a uk or overseas dataset
        if "CCOD_" in filePathString:
            parse_uk_records(filePathString)
        elif "OCOD_" in filePathString:
            parse_overseas_records(filePathString)
        else:
            print(f"Invalid file format: {filePath}")
            