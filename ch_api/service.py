# Starts service selected by user

from lock.lock import remove_lock
from ch_api.charge_data.charge_data import scrape_charge_data
from ch_api.for_co_owner.for_co_owner import  scrape_for_co_owner_data
from ch_api.uk_co_owner.uk_co_owner import  scrape_uk_co_owner_data
from ch_api.utils.output_file import do_output_files_exist
from ch_api.utils.repl_helper import prompt_user_to_continue
from ch_api.ingestion.csv_ingestor import ingest_file
from ch_api.utils.db_methods import fetch_company_list_from_db
from config import config
from typing import Set, Tuple
import os

def does_uk_company_list_exist() -> dict[str, Set[Tuple[int, int]]]:
    if os.path.exists(config.UNIQUE_UK_COMPANIES_LIST_PATH):
        with open(config.UNIQUE_UK_COMPANIES_LIST_PATH, "r", encoding="utf-8") as file:
            for i, line in enumerate(file):
                if i >= 10: 
                    return True
            return False
    else:
        return False

def does_overseas_company_list_exist() -> dict[str, Set[Tuple[int, int]]]:
    
    if os.path.exists(config.UNIQUE_FOREIGN_COMPANIES_LIST_PATH):
        with open(config.UNIQUE_FOREIGN_COMPANIES_LIST_PATH, "r", encoding="utf-8") as file:
            for i, line in enumerate(file):
                if i >= 10:  # We want at least 10 lines, so if we reach n-1, we have enough
                    return True
            return False
    else:
        return False
    
async def start_ch_service(arg: str) -> None: # Starts user service selected by user
    try:
        print("Program started. Running...")
        
        # Create unique companies lists if none exists
        if not does_uk_company_list_exist():
            
            # Get list of uk companies from database
            try:
                fetch_company_list_from_db("uk")  
                print("Successfully fetched unique UK companies list")
            except Exception as e:
                print(e)
                raise Exception("Some error fetching unique UK companies... ")
        
        if not does_overseas_company_list_exist():
            # Get list of foreign companies from database
            try:
                fetch_company_list_from_db("overseas")  
                print("Successfully fetched unique foreign companies list")
            except Exception as e:
                print(e)
                raise Exception("Some error fetching companies... ")
        
        #  If they exist, prompt user to recollect unique company list
        if does_uk_company_list_exist():
            if prompt_user_to_continue("Do you want to recollect unique uk company list?"):
                fetch_company_list_from_db("uk", update=True) 
        
        if does_overseas_company_list_exist():
            if prompt_user_to_continue("Do you want to recollect unique foreign company list?"):
                fetch_company_list_from_db("overseas", update=True) 
        
        # Check if there are any API request results left to ingest into database
        output_files = do_output_files_exist()
        
        if len(output_files) > 0:
            if prompt_user_to_continue("Output files exist. Do you want to ingest them?"):
                for file in output_files:
                    if file == config.CHARGE_DATA_OUTPUT_FILE:
                        await ingest_file(file, "charge")
                    if file == config.UK_OWNER_DATA_OUTPUT_FILE:
                        await ingest_file(file, "uk")
                    if file == config.FOR_OWNER_DATA_OUTPUT_FILE: 
                        await ingest_file(file, "overseas")
        
        print(arg)
        # Initiate chosen service
        if arg == "charge":
            await scrape_charge_data()
        elif arg == "uk":
            await scrape_uk_co_owner_data()
        elif arg == "for":
            await scrape_for_co_owner_data()
        else:
            return
    
    except Exception as e:
        print(e)
        raise(e)
        
    finally: 
        remove_lock()
        