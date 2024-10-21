import csv
import os
from database.db_connection import get_connection
from database.db_methods import sanitize_column_names, sanitize_item
from config import config
from datetime import datetime
from ch_api.utils.repl_helper import prompt_user_to_continue
from lock.lock import create_lock, remove_process

async def ingest_file(file_path, output_type):
    
    create_lock("ingest_csv")
    
    if output_type == "charge":
        parse_charge_owner_output(file_path)
        
        response = prompt_user_to_continue("Would you like to remove output file?")
        if(response):
            try:
                os.remove(file_path)
            except Exception as e:
                print("Error removing file")
        else:
            pass
            
    if output_type == "uk": 
        parse_uk_psc_output(file_path)
        response = prompt_user_to_continue("Would you like to remove output file?")
        if(response):
            try:
                os.remove(file_path)
            except Exception as e:
                print("Error removing file")
        else:
            pass
        
    if output_type == "overseas":
        parse_overseas_psc_output(file_path)
        response = prompt_user_to_continue("Would you like to remove output file?")
        if(response):
            try:
                os.remove(file_path)
            except Exception as e:
                print("Error removing file")
        else:
            pass
    
    remove_process()

def get_date() -> str:
    # Get the current date
    current_date = datetime.now().date()

    # Format the date as a string (e.g., 'YYYY-MM-DD')
    formatted_date = current_date.strftime('%Y-%m-%d')
    formatted_date = f", '{formatted_date}'"
    
    return formatted_date

def parse_charge_owner_output(file_path):

    batch_size = 1000
    #Track number of rows added
    row_count = 20750

    try:

        #GEt file date string 
        date_str = get_date()

        # Connect to the database
        connection = get_connection()
        connection.start_transaction()  
        cursor = connection.cursor()     

        # Open and read the CSV file
        with open(file_path, mode='r', encoding="utf-8", buffering=8192) as file:
            reader = csv.reader(file)
            fileHeaderRow = config.UK_COMPANY_CHARGE_HEADER

            query = 'INSERT INTO ch_charge_data '

            fileHeaderRowString = ', '.join([f"`{header}`" for header in fileHeaderRow])
            fileHeaderRowString += ', `Dataset_date`'

            fileHeaderRowSQLString = f"({fileHeaderRowString}) VALUES "

            query += fileHeaderRowSQLString

            values = []
            
            #Determine where to start reading from in csv file
            for _ in range(row_count): 
                next(reader)

            for row in reader:

                row_count += 1

                # Insert each row into the database
                sanitised_items = [sanitize_item(item) for item in row]

                sql_items = ", ".join(
                    f"'{item}'" if item is not None else "Null" 
                    for item in sanitised_items
                )
                
                sql_items += date_str

                values.append(f'({sql_items})')

                if row_count % batch_size == 0:
                    newQuery = query + ', '.join(values[1:]) + ';'
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

        print(f"Successfully ingested {file_path}")
        
        
    
    except Exception as e:
        
        # Rollback transaction
        # Handle any exceptions that may occur
        print(f"Failed to ingest {file_path}: {e}")
        connection.rollback()
    
    finally:
        cursor.close()
        connection.close()

def parse_uk_psc_output(file_path):
    
    batch_size = 100000
    row_count = 0
    
    try:
        #GEt file date string 
        date_str = get_date()

        # Connect to the database
        connection = get_connection()
        connection.start_transaction()
        cursor = connection.cursor()

        # Open and read the CSV file
        with open(file_path, mode='r', encoding="utf-8", buffering=8192) as file:
            reader = csv.reader(file)
            fileHeaderRow = sanitize_column_names(next(reader))  # Get header and sanitize to match with MySql db headers

            query = 'INSERT INTO uk_co_psc '
        
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
        
        print(f"Successfully ingested {file_path}")
    
    except Exception as e:
        # Handle any exceptions that may occur
        print(f"Failed to ingest {file_path}: {e}, Finished at {row_count}")
        
        # Rollback transaction
        connection.rollback()

    finally:
        cursor.close()
        connection.close()

def parse_overseas_psc_output(file_path):
    
    batch_size = 100000
    row_count = 0
    
    try:

        #GEt file date string 
        date_str = get_date()

        # Connect to the database
        connection = get_connection()
        cursor = connection.cursor()

        # Open and read the CSV file
        with open(file_path, mode='r', encoding="utf-8", buffering=8192) as file:
            reader = csv.reader(file)
            fileHeaderRow = sanitize_column_names(next(reader))  # Get header and sanitize to match with MySql db headers

            query = 'INSERT INTO overseas_co_psc '
        
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
        
        print(f"Successfully ingested {file_path}")
        
    
    except Exception as e:
        # Handle any exceptions that may occur
        print(f"Failed to ingest {file_path}: {e}, Finished at {row_count}")
        
        # Rollback transaction
        connection.rollback()
    
    finally:
        cursor.close()
        connection.close()
           