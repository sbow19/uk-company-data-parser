# Starts service selected by user

from lock.lock import create_lock, remove_process
from land_reg.services.data_service import process_directory
from database.db_methods import check_database_connection

def start_hmlr_service(arg: str) -> None: # Starts user service selected by user
    try:
        
        # Check if database is accessible
        if not check_database_connection():
            raise Exception("Could not access database")
            
        # Create the lock before starting the program
        create_lock("ingest_csv")
        print("Program started. Running...")
        
        # Initiate chosen service
        if arg == "both":
            process_directory(arg)
        elif arg == "dom":
            # Ingest UK company datasets
            process_directory(arg)
        elif arg == "for":
            # Ingest overseas company datasets
            process_directory(arg)
        else:
            return
    
    except Exception as e:
        print(str(e)[:1000])
        
    finally: 
        remove_process()