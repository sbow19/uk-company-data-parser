# Starts service selected by user

from lock.lock import create_lock, remove_process
from land_reg.services.data_service import process_directory

def start_hmlr_service(arg: str) -> None: # Starts user service selected by user
    try:
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
        print(e)
        
    finally: 
        remove_process()