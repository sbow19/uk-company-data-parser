import csv
import re
from database.db_connection import get_connection

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

    sanitised_item = item.replace("'", "''")
    sanitised_item = sanitised_item.replace('"', '""')
    sanitised_item = sanitised_item.replace("£", "GBP")
    sanitised_item = sanitised_item.replace("True", "1")
    sanitised_item = sanitised_item.replace("False", "0")
    sanitised_item = sanitised_item.replace("\\", "\\\\")
    
    if len(sanitised_item) < 1:
        sanitised_item = None
    
    

    return sanitised_item

# Create table in MYSQL instance. 
def create_table(db_name, table_name, filePath):

    # Establish db connection with credentials in .env file
    connection = get_connection()

    connection.cursor().execute(f"USE {db_name}")

    columns = []
    # Open and read the CSV file header
    with open(filePath, mode='r') as file:
        reader = csv.reader(file)
        # Read the  first line of the csv file for header
        headers = next(reader)
        # Remove non-alphanumeric characters and white spaces
        columns = sanitize_column_names(headers)
    
    # Dynamically create the CREATE TABLE SQL query based on CSV columns
    columns_with_types = ", ".join([f"{col} VARCHAR(255)" for col in columns])

    columns_with_types += ", Dataset_date DATE"
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_with_types})"
    connection.cursor().execute(query)

    connection.close()

    #Return column headers
    return columns