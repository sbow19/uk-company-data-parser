# SQL query templates for ingestion and other methods

import pandas as pd
import numpy as np
import logging
import sys

'''
    Freelance database is broken into multiple tables, so insert statement is a 
    combination of multiple insert statements to the tables
'''


logging.basicConfig(
    filename='sqlalchemy_errors_result.log',  # Log file name
    filemode='a',   
    level=logging.ERROR,  # Change to DEBUG for more detailed output
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('sqlalchemy')

address_ids_names = [
        "Property Address",
        "property_address_id",
        
        "Proprietor (1) Address (1)",
        "proprietor_1_address_1_id",
        "Proprietor (1) Address (2)",
        "proprietor_1_address_2_id",
        "Proprietor (1) Address (3)",
        "proprietor_1_address_3_id",
        
        "Proprietor (2) Address (1)",
        "proprietor_2_address_1_id",
        "Proprietor (2) Address (2)",
        "proprietor_2_address_2_id",
        "Proprietor (2) Address (3)",
        "proprietor_2_address_3_id",
        
        "Proprietor (3) Address (1)",
        "proprietor_3_address_1_id",
        "Proprietor (3) Address (2)",
        "proprietor_3_address_2_id",
        "Proprietor (3) Address (3)",
        "proprietor_3_address_3_id",
        
        "Proprietor (4) Address (1)",
        "proprietor_4_address_1_id",
        "Proprietor (4) Address (2)",
        "proprietor_4_address_2_id",
        "Proprietor (4) Address (3)",
        "proprietor_4_address_3_id"
]

proprietors_ids = [
    "Proprietor Name (1)",
    "proprietor_1_id",
    "Company Registration No. (1)",
    "Country Incorporated (1)",
    
    "Proprietor Name (2)",
    "Company Registration No. (2)",
    "proprietor_2_id",
    "Country Incorporated (2)",
    
    "Proprietor Name (3)",
    "Company Registration No. (3)",
    "proprietor_3_id",
    "Country Incorporated (3)",

    "Proprietor Name (4)",
    "Company Registration No. (4)",
    "proprietor_4_id",
    "Country Incorporated (4)"
    
]

titles_tenures_ids = [
    "Title Number",
    "Tenure",
    "property_address_id"
]

proprietors_ids_address_ids = [
    "proprietor_1_id",
    "proprietor_1_address_1_id",
    "proprietor_1_address_2_id",
    "proprietor_1_address_3_id",
    
    "proprietor_2_id",
    "proprietor_2_address_1_id",
    "proprietor_2_address_2_id",
    "proprietor_2_address_3_id",
    
    "proprietor_3_id",
    "proprietor_3_address_1_id",
    "proprietor_3_address_2_id",
    "proprietor_3_address_3_id",
    
    "proprietor_4_id",
    "proprietor_4_address_1_id",
    "proprietor_4_address_2_id",
    "proprietor_4_address_3_id"
]

ownership_records = [
    "Title Number",
    "proprietor_1_id",
    "proprietor_2_id",
    "proprietor_3_id",
    "proprietor_4_id",
    "Price Paid",
    "Date Proprietor Added",
    "dataset_date"
]

overseas_company_records = [
    "proprietor_1_id",
    "proprietor_2_id",
    "proprietor_3_id",
    "proprietor_4_id"
]

def uk_parse_statements(records_batch_df):

    address_query = __create_address_query(records_batch_df[address_ids_names])
    proprietors_query = __create_proprietors_query(records_batch_df[proprietors_ids], False)
    titles_query = __create_titles_query(records_batch_df[titles_tenures_ids])
    proprietor_addresses_query = __create_proprietor_addresses_query(records_batch_df[proprietors_ids_address_ids])
    ownership_records_query = __create_ownership_records_query(records_batch_df[ownership_records])
    
    final_queries = [address_query, proprietors_query, titles_query, proprietor_addresses_query, ownership_records_query]
    
    return final_queries

def overseas_parse_statements(records_batch_df):
    
    address_query = __create_address_query(records_batch_df[address_ids_names])
    proprietors_query, result_df_1 = __create_proprietors_query(records_batch_df[proprietors_ids], True)
    
    titles_query = __create_titles_query(records_batch_df[titles_tenures_ids])
    proprietor_addresses_query, result_df_2 = __create_proprietor_addresses_query(records_batch_df[proprietors_ids_address_ids])
    ownership_records_query = __create_ownership_records_query(records_batch_df[ownership_records])
    foreign_details_query = __create_foreign_details_query(records_batch_df[overseas_company_records])
    
    result_df_1 = result_df_1["id"]
    result_df_2 = result_df_2["proprietor_id"]
    
    values_in_df2_not_in_df1 = result_df_1[~result_df_1.isin(result_df_2)].unique()
    
    print("Values in df2['id'] not in df1['proprietorid']:", values_in_df2_not_in_df1)
    
    final_queries = [address_query, proprietors_query, titles_query, proprietor_addresses_query, ownership_records_query, foreign_details_query]
    
    return final_queries

def __create_address_query(addresses_df): 
    
    base_query = """
        INSERT INTO addresses (address, id) 
        VALUES
    """
    
    ### MELT DATAFRAME ###
    
    # Step 1: Create a new DataFrame for the results
    result_df = pd.DataFrame(columns=["address", "id"])
    
    # Step 2: Iterate over each address-ID pair and append to the new DataFrame
    for i in range(1, 5):  # Assuming there are 4 proprietors
        for j in range(1, 4):  # Assuming each proprietor has 3 addresses
            address_col = f"Proprietor ({i}) Address ({j})"
            id_col = f"proprietor_{i}_address_{j}_id"
            
            # Create a temporary DataFrame for the current pair
            temp_df = addresses_df[[address_col, id_col]].copy()
            temp_df.columns = ["address", "id"]  # Rename columns
            
            # Append to the result DataFrame
            result_df = pd.concat([result_df, temp_df], ignore_index=True)
     
    # Replace string 'nan' with actual NaN
    result_df.replace('NAN', np.nan, inplace=True)   
    result_df = result_df.dropna(subset=["address"])

    # Step 3: add Property Address Column
    address_col = "Property Address"
    id_col = "property_address_id"
    
    # Create a temporary DataFrame for the current pair
    temp_df = addresses_df[[address_col, id_col]].copy()
    temp_df.columns = ["address", "id"]  # Rename columns
    
    # Append to the result DataFrame
    result_df = pd.concat([result_df, temp_df], ignore_index=True)    
    
    result_df = result_df.drop_duplicates()
    
    ### CREATE QUERY ###
    query = base_query + ", ".join(f"('{row['address']}', '{row['id']}')" for _, row in result_df.iterrows())
    
    query += "ON DUPLICATE KEY UPDATE id = VALUES(id), address = VALUES(address);"
    
    return query

def __create_proprietors_query(proprietors_df, is_overseas):
    
    base_query = """
        INSERT INTO proprietors (id, name, registration_number, is_foreign_co, country_incorporated) 
        VALUES
    """
    
    ### MELT DATAFRAME ###
    
    # Step 1: Create a new DataFrame for the results
    result_df = pd.DataFrame(columns=["id", "name", "registration_number", "is_foreign_co", "country_incorporated"])
    
    if is_overseas:
        is_overseas = 1
        # Step 2: Iterate over 
        for i in range(1, 5):  # Assuming there are 4 proprietors
            proprietor_name_col = f"Proprietor Name ({i})"
            proprietors_id_col = f"proprietor_{i}_id"
            proprietors_number_col = f"Company Registration No. ({i})"
            proprietors_country_col = f"Country Incorporated ({i})"
            
            # Create a temporary DataFrame for the current pair
            temp_df = proprietors_df[[proprietors_id_col, proprietor_name_col, proprietors_number_col, proprietors_country_col]].copy()
            temp_df.columns = ["id", "name", "registration_number", "country_incorporated"]  # Rename columns
            temp_df["is_foreign_co"] = is_overseas
            
            # Append to the result DataFrame
            result_df = pd.concat([result_df, temp_df], ignore_index=True)
    else:
        is_overseas = 0
        for i in range(1, 5):  # Assuming there are 4 proprietors
            proprietor_name_col = f"Proprietor Name ({i})"
            proprietors_id_col = f"proprietor_{i}_id"
            proprietors_number_col = f"Company Registration No. ({i})"
            proprietors_country_col = "UNITED KINGDOM"
            
            # Create a temporary DataFrame for the current pair
            temp_df = proprietors_df[[proprietors_id_col, proprietor_name_col, proprietors_number_col]].copy()
            temp_df.columns = ["id", "name", "registration_number"]  # Rename columns
            temp_df["is_foreign_co"] = is_overseas
            temp_df["country_incorporated"] = proprietors_country_col
            
            # Append to the result DataFrame
            result_df = pd.concat([result_df, temp_df], ignore_index=True)

    
    
    # Step 3: Drop rows with empty names and ids
    result_df = result_df[result_df['name'].notna() & (result_df['name'] != '')]
    
    # Ensure 'id' column is treated as a string
    result_df["id"] = result_df["id"].replace('NAN', np.nan) 
    result_df = result_df.dropna(subset=["id"])
    
    # Drop duplicates based on the 'id' column
    result_df = result_df.drop_duplicates(subset=['id'], keep="first")
    
    # #
    print(result_df["id"].nunique())
    duplicates = result_df[result_df.duplicated(subset=['name', 'registration_number', 'country_incorporated'], keep=False)]
    # Print the duplicates
    print(duplicates.to_string())
    
    
    ### CREATE QUERY ###
    query = base_query + ", ".join(f"('{row['id']}', '{row['name']}', '{row['registration_number']}', {row['is_foreign_co']}, '{row['country_incorporated']}')" for _, row in result_df.iterrows())
    query += "ON DUPLICATE KEY UPDATE id = id;"
    
    return query, result_df

def __create_titles_query(titles_df):
    base_query = """
        INSERT INTO titles (title, tenure, address_id) 
        VALUES
    """
    
    ### MELT DATAFRAME ###
    
    # Step 1: Create a new DataFrame for the results
    result_df = pd.DataFrame(columns=["title", "tenure", "address_id"])
    
    temp_df = titles_df[["Title Number", "Tenure", "property_address_id"]].copy()
    temp_df.columns = ["title", "tenure", "address_id"] 
    
    # Append to the result DataFrame
    result_df = pd.concat([result_df, temp_df], ignore_index=True)
    
    ### CREATE QUERY ###
    query = base_query + ", ".join(f"('{row['title']}', '{row['tenure']}', '{row['address_id']}')" for _, row in result_df.iterrows())
    
    query += "ON DUPLICATE KEY UPDATE title = VALUES(title), address_id = VALUES(address_id);"
    
    return query

def __create_proprietor_addresses_query(proprietor_addresses_df):
    base_query = """
        INSERT INTO proprietor_addresses (proprietor_id, address_id) 
        VALUES
    """
    
    ### MELT DATAFRAME ###
    
    # Step 1: Create a new DataFrame for the results
    result_df = pd.DataFrame(columns=["proprietor_id", "address_id"])

    # Step 2: Iterate over proprietors and their address ids
    for i in range(1, 5):  # Assuming there are 4 proprietors
        for j in range(1, 4):
            proprietors_id_col = f"proprietor_{i}_id"
            address_id_col = f"proprietor_{i}_address_{j}_id"
            
            # Create a temporary DataFrame for the current pair
            temp_df = proprietor_addresses_df[[proprietors_id_col, address_id_col]].copy()
            temp_df.columns = ["proprietor_id", "address_id"]  # Rename columns
            
            # Append to the result DataFrame
            result_df = pd.concat([result_df, temp_df], ignore_index=True)

    
    # Step 3: Drop duplicate entries
    result_df = result_df.drop_duplicates()
    
    # Replace string 'NAN' with actual NaN
    result_df['proprietor_id'] = result_df['proprietor_id'].replace('NAN', np.nan) 
    result_df = result_df.dropna(subset=['proprietor_id'])

    result_df['address_id'] = result_df['address_id'].replace('NAN', np.nan) 
    result_df = result_df.dropna(subset=['address_id'])     
    
    print(result_df["proprietor_id"].nunique())
    
    ### CREATE QUERY ###
    query = base_query + ", ".join(f"('{row['proprietor_id']}', '{row['address_id']}')" for _, row in result_df.iterrows())
    
    query += "ON DUPLICATE KEY UPDATE proprietor_id = proprietor_id, address_id = address_id;"
    
    return query, result_df

def __create_ownership_records_query(ownership_records_df):
    base_query = """
        INSERT INTO ownership_records (title, proprietor_id, price_paid, date_proprietor_added, dataset_date) 
        VALUES
    """
    
    ### MELT DATAFRAME ###
    
    # Step 1: Create a new DataFrame for the results
    result_df = pd.DataFrame(columns=["title", "proprietor_id", "price_paid", "date_proprietor_added", "dataset_date"])

    # Step 2: Iterate over proprietors and their address ids
    for i in range(1, 5):  # Assuming there are 4 proprietors
        proprietor_id_col = f"proprietor_{i}_id"
        title_col = "Title Number"
        price_paid_col = "Price Paid"
        date_prop_added = "Date Proprietor Added"
        dataset_date = "dataset_date"
        
        # Create a temporary DataFrame for the current pair
        temp_df = ownership_records_df[[title_col, proprietor_id_col, price_paid_col, date_prop_added, dataset_date]].copy()
        temp_df.columns = ["title", "proprietor_id", "price_paid", "date_proprietor_added", "dataset_date"] 
        
        # Append to the result DataFrame
        result_df = pd.concat([result_df, temp_df], ignore_index=True)

   
    # Step 3: Drop rows with empty names
    
    result_df["proprietor_id"] = result_df["proprietor_id"].replace('NAN', np.nan)
    result_df.dropna(subset=["proprietor_id"], inplace=True)

    
    # Replace string 'nan' with actual NaN
    result_df["price_paid"] = result_df["price_paid"].replace(['NAN', 'N/A', ''], 0)
    result_df["price_paid"] = pd.to_numeric(result_df["price_paid"], errors='coerce').fillna(0)
    
    ### CREATE QUERY ###
    query = base_query + ", ".join(
        f"('{row['title']}', '{row['proprietor_id']}', '{row['price_paid']}', "
        f"{'NULL' if pd.isna(row['date_proprietor_added']) else f'\'{row["date_proprietor_added"]}\' '}, "
        f"'{row['dataset_date']}')"
        for _, row in result_df.iterrows()
    )
    
    query += "ON DUPLICATE KEY UPDATE proprietor_id = proprietor_id;"
        
    return query

def __create_foreign_details_query(foreign_details_df):
    base_query = """
        INSERT INTO foreign_proprietor_details (proprietor_id) 
        VALUES
    """
    
    ### MELT DATAFRAME ###
    
    # Step 1: Create a new DataFrame for the results
    result_df = pd.DataFrame(columns=["proprietor_id"])

    # Step 2: Iterate over proprietors and their address ids
    for i in range(1, 5):  # Assuming there are 4 proprietors
        
        proprietors_id_col = f"proprietor_{i}_id"
        
        # Create a temporary DataFrame for the current pair
        temp_df = foreign_details_df[[proprietors_id_col]].copy()
        temp_df.columns = ["proprietor_id"]  # Rename columns
        
        # Append to the result DataFrame
        result_df = pd.concat([result_df, temp_df], ignore_index=True)

    # Step 3: Drop rows with empty names
    result_df['proprietor_id'] = result_df['proprietor_id'].replace('NAN', np.nan) 
    result_df = result_df.dropna(subset=['proprietor_id'])

    # Drop rows with any NaN values
    result_df = result_df.drop_duplicates(subset=["proprietor_id"])
    
    ### CREATE QUERY ###
    query = base_query + ", ".join(f"('{row['proprietor_id']}')" for _, row in result_df.iterrows())
    
    query +="ON DUPLICATE KEY UPDATE proprietor_id = proprietor_id;"
    
    return query