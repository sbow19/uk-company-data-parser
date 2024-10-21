import re
from database.db_connection import get_connection
from sqlalchemy import text
import sys


def sanitize_column_names(headerColumn):

    sanitisedHeaders = []
    
    for name in headerColumn:
        # Replace spaces with underscores and remove invalid characters
        name = re.sub(r'\s+', '_', name)  # Replace spaces with underscores
        name = re.sub(r'[^\w]', '', name)  # Remove non-alphanumeric characters
        name.strip()
        sanitisedHeaders.append(name)

    return sanitisedHeaders

def sanitize_item(item):
 
    sanitised_item = str(item)
    sanitised_item = sanitised_item.replace("'", "''")
    sanitised_item = sanitised_item.replace("`", " ")
    sanitised_item = sanitised_item.replace(".", " ")
    sanitised_item = sanitised_item.replace("£", "GBP")
    sanitised_item = sanitised_item.replace(":", " ")
    sanitised_item = sanitised_item.replace("%", "%%")
    sanitised_item = sanitised_item.replace("True", "1")
    sanitised_item = sanitised_item.replace("False", "0")
    sanitised_item = sanitised_item.replace("\\", "\\\\")
    
    if len(sanitised_item) < 1:
        sanitised_item = None
    
    return sanitised_item.upper().strip()

# Function to check if database is accessible
def check_database_connection():
    try:
        engine = get_connection()
        with engine.connect() as connection:
            print("Engine connection successful... closing connection")
        
        return True
    except Exception as err:
        print(f"Error: {err}")
        return False

# SQL query builder functions
def execute_queries(sql_queries, params=[]):
    
    engine = get_connection()
    
    try:
        with engine.connect() as connection:
            
            
            for query in sql_queries:
                print(query[:1000])
                connection.execute(text(query))
            connection.commit()     
            return
    
    except Exception as e:
        raise Exception(e)
        
# Fetch address ids as dict
def fetch_address_ids():
    
    engine = get_connection()
    
    with engine.connect() as connection:
        
        result = connection.execute(text("SELECT id, address FROM addresses;"))
        address_dict = {}
        
        for row in result:
            address_dict[row.address] = row.id
            
        
        return address_dict

# Fetch company ids as dict
def fetch_company_ids():
    
    engine = get_connection()
    
    with engine.connect() as connection:
        
        result = connection.execute(text("SELECT id, name, registration_number, country_incorporated FROM proprietors;"))
        company_dict = {
            "names_dict": {},
            "details": {}
        }
        
        no_of_ids = 0
        for row in result:
            no_of_ids += 1
            
            # Stringify contents of database
            id = str(row.id)
            name = sanitize_item(str(row.name))
            registration_number = str(row.registration_number)
            country_incorporated = sanitize_item(str(row.country_incorporated))
            
            company_dict["details"][id] = {
                "id": id,
                "name": name,
                "number": registration_number,
                "country_incorporated": country_incorporated
            }
            
            # Add id to set of ids associated with company name
            if name in company_dict["names_dict"]:
                company_dict["names_dict"][name].add(id)               
            else:
                company_dict["names_dict"][name] = set()
                company_dict["names_dict"][name].add(id) 
        
        print(no_of_ids)
        return company_dict