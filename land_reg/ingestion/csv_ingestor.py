import csv
import re
from database.db_connection import get_connection
from database.db_methods import create_table, sanitize_column_names, sanitize_item
from config import config

def get_date(filePath):

    #Date pattern in regex
    pattern = r'(\d{4})_(\d{2})'

    # Search for the pattern in the file path
    match = re.search(pattern, filePath)
    if match:
        year, month = match.groups()
        # Format the date as YYYY-MM-DD
        date_str = f", '{year}-{month}-01'"  # Assuming the day is the 1st of the month
        return date_str
    else:
        raise ValueError("Date not found in the file path")

def parse_overseas_records(filePath):

    batch_size = 100000
    #Track number of rows added
    row_count = 0

    try:
        create_table('hmlr', "overseas_companies", filePath)

        #GEt file date string 
        date_str = get_date(filePath)

        # Connect to the database
        connection = get_connection()
        cursor = connection.cursor()

        

        # Open and read the CSV file
        with open(filePath, mode='r', encoding="utf-8", buffering=8192) as file:
            reader = csv.reader(file)
            fileHeaderRow = sanitize_column_names(next(reader))  # Get header and sanitize to match with MySql db headers

            query = 'INSERT INTO overseas_companies '
            fileHeaderRowString = ""

            fileHeaderRowString = ', '.join([f"`{header}`" for header in fileHeaderRow])
            fileHeaderRowString += ', Dataset_date'

            fileHeaderRowSQLString = f"({fileHeaderRowString}) VALUES "

            query += fileHeaderRowSQLString

            values = []

            for row in reader:

                row_count += 1

                # Insert each row into the database
                sanitised_items = [sanitize_item(item) for item in row]

                sql_items = ", ".join(f"'{item}'" for item in sanitised_items)
                sql_items += date_str

                values.append(f'({sql_items})')

                if row_count % batch_size == 0:
                    newQuery = query + ', '.join(values) + ';'
                    cursor.execute(newQuery)
                    print(f'{cursor.rowcount} rows inserted successfully.')
                    print(row_count)

                    # Commit the changes to the database
                    connection.commit()
                    values = []
                
            #For remaining rows not in batch
            if values:
                newQuery = query + ', '.join(values[:-1]) + ';' #Join all entries except the last one, where it is the row count
                cursor.execute(newQuery)
                print(f'{cursor.rowcount} rows inserted successfully.')
                print(row_count)

                # Commit the changes to the database
                connection.commit()
    

        # Log the successfully parsed file path to the .txt file
        with open(config.PARSED_FILE_PATH, mode='a') as log:
            log.write(f"{filePath}\n")

        print(f"Successfully ingested {filePath}")
    
    except Exception as e:
        # Handle any exceptions that may occur
        print(f"Failed to ingest {filePath}: {e}")
    
    finally:
        cursor.close()
        connection.close()

def parse_uk_records(filePath):
    
    batch_size = 500000
    row_count = 0
    
    try:

        #Create table if it doesn't exist
        create_table('hmlr', "uk_companies", filePath)

        #GEt file date string 
        date_str = get_date(filePath)

        # Connect to the database
        connection = get_connection()
        cursor = connection.cursor()

        # Open and read the CSV file
        with open(filePath, mode='r', encoding="utf-8", buffering=8192) as file:
            reader = csv.reader(file)
            fileHeaderRow = sanitize_column_names(next(reader))  # Get header and sanitize to match with MySql db headers

            query = 'INSERT INTO uk_companies '
        
            fileHeaderRowString = ', '.join([f"`{header}`" for header in fileHeaderRow])
            fileHeaderRowString += ', Dataset_date'

            fileHeaderRowSQLString = f"({fileHeaderRowString}) VALUES "

            query += fileHeaderRowSQLString

            values = []

            #Determine where to start reading from in csv file
            for _ in range(row_count): 
                next(reader)

            #Read throuhg csv file in batches
            for row in reader:

                row_count += 1
                # Insert each row into the database
                sanitised_items = [sanitize_item(item) for item in row]

                sql_items = ", ".join(f"'{item}'" for item in sanitised_items)
                sql_items += date_str

                values.append(f'({sql_items})')

                if row_count % batch_size == 0:
                    newQuery = query + ', '.join(values) + ';'
                    cursor.execute(newQuery)
                    print(f'{cursor.rowcount} rows inserted successfully.')
                    print(row_count)

                    # Commit the changes to the database
                    connection.commit()
                    values = []

            #For remaining rows not in batch
            if values:
                newQuery = query + ', '.join(values[:-1]) + ';' #Join all entries except the last one, where it is the row count
                cursor.execute(newQuery)
                print(f'{cursor.rowcount} rows inserted successfully.')
                print(row_count)

                # Commit the changes to the database
                connection.commit()
        
        # Log the successfully parsed file path to the .txt file
        with open(config.PARSED_FILE_PATH, mode='a') as log:
            log.write(f"{filePath}\n")

        print(f"Successfully ingested {filePath}")
    
    except Exception as e:
        # Handle any exceptions that may occur
        print(f"Failed to ingest {filePath}: {e}, Finished at {row_count}")


    
    finally:
        cursor.close()
        connection.close()

def ingest_csv_files(filePathArray):

    for filePath in filePathArray:

        filePathString = str(filePath)
        #Check which dataset we are using
        if "CCOD_" in filePathString:
            parse_uk_records(filePathString)
        elif "OCOD_" in filePathString:
            parse_overseas_records(filePathString)
        else:
            print(f"Invalid file format: {filePath}")
            