from ch_api.utils.output_file import (
    create_uk_co_owners_data_output_files,
    read_company_list_file,
    trim_company_list_file,
    read_failed_company_list_file,
    does_failed_list_exist,
    does_finished_list_exist
)
from ch_api.uk_co_owner.utils.scrape_data import Scrape_UK_Company_Owners
from ch_api.utils.repl_helper import prompt_user_to_continue
from ch_api.utils.misc import split_into_chunks
import aiohttp
import asyncio
from lock.lock import create_lock, remove_process
import multiprocessing
from lock.lock import get_available_api_keys, create_api_key_lock, remove_api_key_from_lock

async def scrape_unique(lock, company_list_chunk, pbar_position):
    
    # Check how many tasks can be run based on api keys available
    api_keys = get_available_api_keys()
    
    if len(api_keys) < 1:
        raise Exception("At least one api key is required")
    
    create_api_key_lock("fetch_for_co_bo", api_keys[0])
    
    # Main scrape loop
    while True:
        
        retry_attempts = 0
        max_retries = 100
        backoff_factor = 2
            
        try:
            # Initial Scrape data from companies house
            Scrape_UK_Company_Owners.initialize_class(lock)
            scraper = Scrape_UK_Company_Owners(api_keys[0])
            await scraper.scrape_data(company_list_chunk, pbar_position)
            print("Successfully scraped company records")
        except aiohttp.ClientConnectionError as e:
            # Handle connection errors (e.g., DNS failures, refused connections)
            print(f"Client connection error: {e}")
            retry_attempts += 1
            if retry_attempts >= max_retries:
                print("Max retries reached. Giving up.")
                raise
            delay = backoff_factor ** retry_attempts
            print(f"Retrying in {delay} seconds...")
            await asyncio.sleep(delay)
        except aiohttp.ClientError as e:
            # If there is a Client Error, just restart the process again
            continue
        except Exception as e:
            print(e)
            raise Exception("Some error fetching data from Companies House...")
        finally:
            # Release api keys
            remove_api_key_from_lock(api_keys[0])
        
        break  

async def scrape_failed(lock, company_list_chunk, pbar_position):
    # Check how many tasks can be run based on api keys available
    api_keys = get_available_api_keys()
    
    if len(api_keys) < 1:
        raise Exception("At least one api key is required")
    
    create_api_key_lock("fetch_for_co_bo", api_keys[0])
    
    # Main scrape loop
    while True:
        
        retry_attempts = 0
        max_retries = 100
        backoff_factor = 2
            
        try:
            # Initial Scrape data from companies house
            Scrape_UK_Company_Owners.initialize_class(lock)
            scraper = Scrape_UK_Company_Owners(api_keys[0])
            await scraper.scrape_failed_data(company_list_chunk, pbar_position)
            print("Successfully scraped company records")
        except aiohttp.ClientConnectionError as e:
            # Handle connection errors (e.g., DNS failures, refused connections)
            print(f"Client connection error: {e}")
            retry_attempts += 1
            if retry_attempts >= max_retries:
                print("Max retries reached. Giving up.")
                raise
            delay = backoff_factor ** retry_attempts
            print(f"Retrying in {delay} seconds...")
            await asyncio.sleep(delay)
        except aiohttp.ClientError as e:
            # If there is a Client Error, just restart the process again
            continue
        except Exception as e:
            print(e)
            raise Exception("Some error fetching data from Companies House...")
        finally:
            # Release api keys
            remove_api_key_from_lock(api_keys[0])
        
        break  

def run_scrape(process, lock, company_list_chunk, pbar_position):
    print("Running scrape now...")
    
    if process == "unique":
        asyncio.run(scrape_unique(lock, company_list_chunk, pbar_position))
    elif process == "failed":
        asyncio.run(scrape_failed(lock, company_list_chunk, pbar_position))

def unique_list_scrape():
    # Check if there is already data in the company list, else, just read from text file
    company_list = {}
    
    # Full unique UK company list, without being narrowed down
    company_list["uk_companies"] = read_company_list_file("dom")
    
    # Check if there is a finished or unfinished company list
    if does_finished_list_exist("uk_co_bo"):
        if prompt_user_to_continue("Would you like to narrow down the unique company list with these additional lists"):
            try:
                # unique UK company list narrowed down by failed and finished list.
                company_list["uk_companies"] = trim_company_list_file(company_list["uk_companies"], "dom")
            except Exception as e:
                print(e)
                raise Exception("Some error trimming unique company names list...")
        
    # Create a lock object
    lock = multiprocessing.Lock()

    processes = []
        
    # Convert set to a list
    company_listified = list(company_list["uk_companies"])
        
    # Split company_list into three chunks
    chunks = split_into_chunks(company_listified, 3)
    pbar_position = 0
    # Call the method for each chunk
    for chunk in chunks:
        
        p = multiprocessing.Process(target=run_scrape, args=("unique", lock, chunk, pbar_position))
        processes.append(p)
        p.start()
        pbar_position += 1

    # Ensure all processes have completed
    for p in processes:
        p.join()

async def failed_list_scrape():
    
    company_list = {}
    
    # Failed list companies
    company_list["uk_companies"] = read_failed_company_list_file("uk_bo")
    
    # Create a lock object
    lock = multiprocessing.Lock()

    processes = []
        
    # Convert set to a list
    company_listified = list(company_list["uk_companies"])
        
    # Split company_list into three chunks
    # Split company_list into two chunks
    chunks = split_into_chunks(company_listified, 3)
    pbar_position = 0
    # Call the method for each chunk
    for chunk in chunks:
        
        p = multiprocessing.Process(target=run_scrape, args=("failed", lock, chunk, pbar_position))
        processes.append(p)
        p.start()
        pbar_position += 1

    # Ensure all processes have completed
    for p in processes:
        p.join()

# Handles REPL for scraping charge data
async def scrape_uk_co_owner_data():
 
    
    create_lock("fetch_uk_co_bo")
    
    #Generates progress tracker files
    create_uk_co_owners_data_output_files()
    
    while True:

        # Check if failed list exists, then attempt to scrape that
        if does_failed_list_exist("uk_co_bo"):
            if prompt_user_to_continue("Failed list exists, do you want to attempt to scrape this data"):
                await failed_list_scrape()

        if prompt_user_to_continue("Would you like to scrape unique company list?"):
            unique_list_scrape()
        else:
            break
    
    remove_process()