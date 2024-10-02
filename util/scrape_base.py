import pandas as pd
from config import config
from ch_api.utils.progress import time_progress
from typing import Optional, Set, Tuple, List, Literal
from tqdm.asyncio import tqdm_asyncio
from ch_api.utils.async_http import concurrent_http_request_pool
from util.api_response_parse_schemas import (
    charge_data_parse_schema,
    beneficial_owner_parse_schema,
    company_name_parse_schema,
    company_number_parse_schema
)
import aiohttp
import time
import os

class Scrape_Data: 
    
    def __init__(self, api_key, process_type):
        self.data_list = []                                                     # List of scraped data
        self.company_queue = set()                                              # Queue of company details
        self.company_queue_post_name_fetch: Set[Tuple[str, str]] = set()        # (Optional) Queue of company details after name fetch
        self.company_name_fetch_queue = set()                                   # (Optional) Queue for queries to CH search API                
        self.successful_company_query = set()                                   # Set of tuples of company name/number pairs to be added to success list
        self.failed_company_fetch = set()                                       # Set of tuples of company name/number pairs to be added to failed list                                        
        self.api_key = api_key                                                  # Api key used to access CH resources
        self.process_type = process_type     
        self.prev_successful_queries_list = set()                               # (Optional) Prev results compared to current name searches to avoid duped calls 
        # Property determines flow of requests
        
        # Set specific scraping parameters by virtue of process type
        if process_type in ["charge_data", "failed_charge_queries"]:
            self.df = pd.DataFrame(columns=config.UK_COMPANY_CHARGE_HEADER)
            
            self.failed_query_output_file = config.CHARGE_DATA_UNSUCCESSFUL_QUERIES_PATH
            self.successful_query_output_file = config.CHARGE_DATA_FINISHED_QUERIES_PATH
            self.results_output_file = config.CHARGE_DATA_OUTPUT_FILE
        elif process_type in ["for_co_owner", "missed_for_co_number"]:
            self.df = pd.DataFrame(columns=config.COMPANY_BO_HEADER)
            self.failed_query_output_file = config.FOR_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH
            self.successful_query_output_file = config.FOR_OWNER_DATA_FINISHED_QUERIES_PATH 
            self.results_output_file = config.FOR_OWNER_DATA_OUTPUT_FILE  
        
        elif process_type in ["uk_co_owner", "failed_uk_bo_queries"]:
            self.df = pd.DataFrame(columns=config.COMPANY_BO_HEADER)
            self.failed_query_output_file = config.UK_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH
            self.successful_query_output_file = config.UK_OWNER_DATA_FINISHED_QUERIES_PATH
            self.results_output_file = config.UK_OWNER_DATA_OUTPUT_FILE
        else:
            raise ValueError("Invalid process type. Please choose from 'charge_data', 'for_co_owner', or 'uk_co_owner'.")
        
    
    # Class variables specific to all
    chunk_size = 100                     # Number of  companies per batch for API requests
    lock = None                          # Lock for managing shared resources between processes
    
    @classmethod
    def initialize_class(cls, lock):
        cls.lock = lock                  # Set class-level lock for concurrent requests
    
    
    # Fetch company information
    async def scrape_data(self, company_details_list=set()):
        
        # Check which process is being done
        if self.process_type == "charge_data":
            await self.__fetch_charge_data_wrapper(company_details_list)
        elif self.process_type == "for_co_owner":
            await self.__fetch_for_co_owner_data_wrapper(company_details_list)
        elif self.process_type == "uk_co_owner":
            await self.__fetch_uk_co_owner_data_wrapper(company_details_list)
        elif self.process_type == "missed_for_co_number":
            await self.__fetch_missed_company_number_wrapper(company_details_list)
        elif self.process_type in ["failed_charge_queries", "failed_uk_bo_queries"]:
            await self.__fetch_failed_queries_wrapper(company_details_list)
        else:
            raise ValueError("Invalid process type. Please choose from 'charge_data', 'for_co_owner', or 'uk_co_owner'.")"
        
    
    # Process type wrappers
    @time_progress("Overseas Companies Beneficial Owners Scrape", "Company")
    async def __fetch_for_co_owner_data_wrapper(self, company_details_list=set(), pbar: Optional[tqdm_asyncio]=None):
       
        for company_details in company_details_list:
                
                self.company_queue.add(company_details)
                
                # Once the length of the chunk reaches 1000, then we provide the company ids to the fetch data method
                if len(self.company_queue) == self.chunk_size:
                    
                    try:
                        # Try searching for companies in search
                        await self.__fetch_co_number_data_wrapper(self.company_queue)
        
                        # Get
                        await self.__fetch_bo_data_wrapper(self.company_queue_post_name_fetch)
                        # Save company data to csv

                        Scrape_Data.__save_to_csv(self.data_list, self.results_output_file)
                        
                        # Update companies finished list file
                        Scrape_Data.__write_finished_queries(self.successful_company_query, self.successful_query_output_file)
                        
                        # Update list of failed queries
                        Scrape_Data.__write_unsuccessful_queries(self.failed_company_fetch, self.failed_query_output_file)
                        
                    except aiohttp.ClientConnectionError as e:
                        print(e)
                        raise e
                    except aiohttp.ClientError as e:
                        print(e)
                        raise e                
                    except Exception as e:
                        print(e)
                    
                    finally:
                        self.company_queue = set()
                        self.successful_company_query = set()
                        self.data_list = []
                        self.failed_company_fetch = set()
                        self.company_queue_post_name_fetch = set()
                    
                        pbar.update(Scrape_Data.chunk_size)
                        pbar.refresh()
                        continue
        
        try:
            # Try searching for companies in search
            await self.__fetch_co_number_data_wrapper(self.company_queue)

            # Get
            await self.__fetch_bo_data_wrapper(self.company_queue_post_name_fetch)
            # Save company data to csv

            Scrape_Data.__save_to_csv(self.data_list, self.results_output_file)
                        
            # Update companies finished list file
            Scrape_Data.__write_finished_queries(self.successful_company_query, self.successful_query_output_file)
            
            # Update list of failed queries
            Scrape_Data.__write_unsuccessful_queries(self.failed_company_fetch, self.failed_query_output_file)
                        
            
        except aiohttp.ClientConnectionError as e:
            print(e)
            raise e
        except aiohttp.ClientError as e:
            print(e)
            raise e                
        except Exception as e:
            print(e)
        
        finally:
            self.company_queue = set()
            self.successful_company_query = set()
            self.data_list = []
            self.failed_company_fetch = set()
            self.company_queue_post_name_fetch = set()
        
            pbar.update(Scrape_Data.chunk_size)
            pbar.refresh()
     
    @time_progress("Companies House UK Company PSC Scrape", "Company")
    async def __fetch_uk_co_owner_data_wrapper(self, company_details_list=set(), pbar: Optional[tqdm_asyncio]=None):
    
        blank_companies = set()                              
        
        for company_details in company_details_list:
        
            # Skip company details if no company number 
            if len(company_details[1]) == 0:
                blank_companies.add(company_details)
                self.failed_company_fetch.add(company_details)
                if len(blank_companies) == 100:
                    pbar.update(100)
                    pbar.refresh()
                    blank_companies = set()
                continue
            
            self.company_queue.add(company_details)
            
            # Once the length of the chunk reaches 1000, then we provide the company ids to the fetch data method
            if len(self.company_queue) == self.chunk_size:
            
                try:
                    await self.__fetch_bo_data_wrapper(self.company_queue)    # Company number is second object in tuple
                    # Save company data to csv

                    Scrape_Data.__save_to_csv(self.data_list, self.results_output_file)
                        
                    # Update companies finished list file
                    Scrape_Data.__write_finished_queries(self.successful_company_query, self.successful_query_output_file)
                    
                    # Update list of failed queries
                    Scrape_Data.__write_unsuccessful_queries(self.failed_company_fetch, self.failed_query_output_file)
                        
                except aiohttp.ClientConnectionError as e:
                    raise e
                except aiohttp.ClientError as e:
                    raise e                
                except Exception as e:
                    print(e)
                
                finally:
                    self.company_queue = set()
                    self.successful_company_query = set()
                    self.data_list = []
                    self.failed_company_fetch = set()
                    
                    pbar.update(Scrape_Data.chunk_size)
                    pbar.refresh()
                    continue
            
        try:
            await self.__fetch_bo_data_wrapper(self.company_queue)    # Company number is second object in tuple
            # Save company data to csv

            Scrape_Data.__save_to_csv(self.data_list, self.results_output_file)
                        
            # Update companies finished list file
            Scrape_Data.__write_finished_queries(self.successful_company_query, self.successful_query_output_file)
            
            # Update list of failed queries
            Scrape_Data.__write_unsuccessful_queries(self.failed_company_fetch, self.failed_query_output_file)
                        
            
        except aiohttp.ClientConnectionError as e:
            raise e
        except aiohttp.ClientError as e:
            raise e                
        except Exception as e:
            print(e)
        
        finally:
            self.company_queue = set()
            self.successful_company_query = set()
            self.data_list = []
            self.failed_company_fetch = set()
            
            pbar.update(Scrape_Data.chunk_size)
            pbar.refresh()
    
    @time_progress("Companies House Charge Scrape", "Company")
    async def __fetch_charge_data_wrapper(self, company_details_list=set(), pbar: Optional[tqdm_asyncio]=None):
    
        blank_companies = set()                              
        
        for company_details in company_details_list:
        
            # Skip company details if no company number 
            if len(company_details[1]) == 0:
                blank_companies.add(company_details)
                self.failed_company_fetch.add(company_details)
                if len(blank_companies) == 100:
                    pbar.update(100)
                    pbar.refresh()
                    blank_companies = set()
                continue
            
            self.company_queue.add(company_details)
            
            # Once the length of the chunk reaches 1000, then we provide the company ids to the fetch data method
            if len(self.company_queue) == self.chunk_size:
            
                try:
                    await self.__fetch_charge_data_wrapper(self.company_queue) 
                    Scrape_Data.__save_to_csv(self.data_list, self.results_output_file)
                        
                    # Update companies finished list file
                    Scrape_Data.__write_finished_queries(self.successful_company_query, self.successful_query_output_file)
                    
                    # Update list of failed queries
                    Scrape_Data.__write_unsuccessful_queries(self.failed_company_fetch, self.failed_query_output_file)
              
                    
                except aiohttp.ClientConnectionError as e:
                    raise e
                except aiohttp.ClientError as e:
                    raise e                
                except Exception as e:
                    print(e)
                
                finally:
                    self.company_queue = set()
                    self.successful_company_query = set()
                    self.data_list = []
                    self.failed_company_fetch = set()
                    
                    pbar.update(Scrape_Data.chunk_size)
                    pbar.refresh()
                    continue
        
        try:
            await self.__fetch_charge_data_wrapper(self.company_queue)    # Company number is second object in tuple
            # Save company data to csv

            Scrape_Data.__save_to_csv(self.data_list, self.results_output_file)
                    
            # Update companies finished list file
            Scrape_Data.__write_finished_queries(self.successful_company_query, self.successful_query_output_file)
            
            # Update list of failed queries
            Scrape_Data.__write_unsuccessful_queries(self.failed_company_fetch, self.failed_query_output_file)
                
                
        except aiohttp.ClientConnectionError as e:
            raise e
        except aiohttp.ClientError as e:
            raise e                
        except Exception as e:
            print(e)
            
        finally:
            self.company_queue = set()
            self.successful_company_query = set()
            self.data_list = []
            self.failed_company_fetch = set()
            
            pbar.update(Scrape_Data.chunk_size)
            pbar.refresh()
    
    @time_progress("Missed Overseas Companies Beneficial Owners Scrape", "Company")
    async def __fetch_missed_company_number_wrapper(self, company_numbers_list=set(), pbar: Optional[tqdm_asyncio]=None):   
        
        for missed_number in company_numbers_list:
            
            missed_number = ("", missed_number)
            self.company_queue.add(missed_number)
            
            # Once the length of the chunk reaches 1000, then we provide the company ids to the fetch data method
            if len(self.company_queue) == self.chunk_size:
                
                try:
                    
                    await self.__fetch_for_co_name_wrapper(self.company_queue)
                    # Get
                    await self.__fetch_bo_data_wrapper(self.company_queue)
                    
                    Scrape_Data.__save_to_csv(self.data_list, self.results_output_file)
                        
                    # Update companies finished list file
                    Scrape_Data.__write_finished_queries(self.successful_company_query, self.successful_query_output_file)
                    
                    # Update list of failed queries
                    Scrape_Data.__write_unsuccessful_queries(self.failed_company_fetch, self.failed_query_output_file)
              
                except aiohttp.ClientConnectionError as e:
                    print(e)
                    raise e
                except aiohttp.ClientError as e:
                    print(e)
                    raise e                
                except Exception as e:
                    print(e)
                
                finally:
                    self.company_queue = set()
                    self.successful_company_query = set()
                    self.data_list = []
                    self.failed_company_fetch = set()
                
                    pbar.update(Scrape_Data.chunk_size)
                    pbar.refresh()
                    continue
        
        # Clear out remaining numbers
        try:
    
            await self.__fetch_for_co_name_wrapper(self.company_queue)
    
            await self.__fetch_bo_data_wrapper(self.company_queue)
            # Save company data to csv

            Scrape_Data.__save_to_csv(self.data_list, self.results_output_file)
                        
            # Update companies finished list file
            Scrape_Data.__write_finished_queries(self.successful_company_query, self.successful_query_output_file)
            
            # Update list of failed queries
            Scrape_Data.__write_unsuccessful_queries(self.failed_company_fetch, self.failed_query_output_file)
        
        except aiohttp.ClientConnectionError as e:
            print(e)
            raise e
        except aiohttp.ClientError as e:
            print(e)
            raise e                
        except Exception as e:
            print(e)
        
        finally:
            self.company_queue = set()
            self.successful_company_query = set()
            self.data_list = []
            self.failed_company_fetch = set()
            self.company_queue_post_name_fetch = set()
        
            pbar.update(Scrape_Data.chunk_size)
            pbar.refresh()
    
    # Failed list scrape methods
    @time_progress("Companies House UK Company PSC Scrape", "Company")
    async def __fetch_failed_queries_wrapper(self, failed_company_list=set(), pbar: Optional[tqdm_asyncio]=None):
        
        
        for company_details in failed_company_list: 
            
            # If there isn't a company number, then we fetch those company details using the search function api from CH
            if len(company_details[1]) == 0:
                self.company_name_fetch_queue.add(company_details)
                if len(self.company_name_fetch_queue) > self.chunk_size:
                    try:
                        # Attempt to get company number by CH search function
                        await self.__fetch_co_number_data_wrapper(self.company_name_fetch_queue)
                    except Exception as e:
                        print(e)
                        continue
            else:
                self.company_queue_post_name_fetch.add(company_details)
            
            # If post fetch name queue reaches chunk size, then we process these companies
            if len(self.company_queue_post_name_fetch) > self.chunk_size:
                
                remove_list = []
                for company in self.company_queue_post_name_fetch:
                        
                        # Remove company with number already successfully fetched list
                        if self.__company_no_in_finished_list(company[1]):   
                            
                            remove_list.append(company_details)
                            self.company_queue_post_name_fetch.discard(company_details)
                        
                # Remove from failed details file, if any
                self.__remove_company_details_from_unsuccessful_file(remove_list, "list")

                if self.process_type == "failed_charge_queries":
                    await self.__fetch_charge_data_wrapper(self.company_queue_post_name_fetch)
                elif self.process_type == "failed_uk_bo_queries":
                    await self.__fetch_uk_co_owner_data_wrapper(self.company_queue_post_name_fetch)
                
                self.__remove_company_details_from_unsuccessful_file(self.company_name_fetch_queue, "list")
                # Empty post name fetch buffer  
                self.company_queue_post_name_fetch = set()
                self.company_name_fetch_queue = set()
        
            
        # If buffer reaches certain length, then we scrape the data in the company queue
        if len(self.company_name_fetch_queue) > 0:
            
            try:
                # Attempt to get company number by CH search function
                await self.__fetch_co_number_data_wrapper(self.company_name_fetch_queue)
            except Exception as e:
                pass  
            
        # If buffer reaches certain length, then we scrape the data in the company queue
        if len(self.company_queue_post_name_fetch) > 0:
            
            remove_list = []
            for company in self.company_queue_post_name_fetch:
                    
                    # Check if company with number already successfully fetched
                    if self.__company_no_in_finished_list(company[1]):   
                        
                        remove_list.append(company_details)
                        self.company_queue_post_name_fetch.discard(company_details)
                    
            # Remove from failed details file
            self.__remove_company_details_from_unsuccessful_file(remove_list, "list")    
            
            if self.process_type == "failed_charge_queries":
                await self.__fetch_charge_data_wrapper(self.company_queue_post_name_fetch)
            elif self.process_type == "failed_uk_bo_queries":
                await self.__fetch_uk_co_owner_data_wrapper(self.company_queue_post_name_fetch)
                
            
            # Finally remove companies in post name fetch queue
            self.__remove_company_details_from_unsuccessful_file(self.company_queue_post_name_fetch, "list")
            
            # Empty post name fetch buffer  
            self.company_queue_post_name_fetch = set()
            self.company_name_fetch_queue = set()
        
    
    # Fetch data wrappers
    @concurrent_http_request_pool(concurrency=2)
    async def __fetch_co_number_data_wrapper(self, company_details, session=None)-> bool:
        
        # list of company details parsed
        try:
            raw_data = await self.__fetch_data_from_api(company_details, session, self.api_key, "co_number")
                        
            parsed_data = self.__parse_api_response(raw_data, company_details, "co_number")
            
            for company in parsed_data:
                # Add company name and lis of parsed search results
                self.company_queue_post_name_fetch.add((company[0], company[1]))  # Company name, company number                
                
        
        except aiohttp.ClientConnectionError as e:
            self.failed_company_fetch.add(company_details)
            print(e)
            raise e
        
        except aiohttp.ClientError as e:
            print("Error with client sesson... closing")
            self.failed_company_fetch.add(company_details)
            raise e
            
        except Exception as e:
            print(e)
            self.failed_company_fetch.add(company_details)
    
    @concurrent_http_request_pool(concurrency=2)
    async def __fetch_bo_data_wrapper(self, company_details, session=None)-> bool:
       
        # list of company details parsed
        try:
            
            raw_data = await self.__fetch_data_from_api(company_details, session, self.api_key, "psc")
            
            parsed_data = self.__parse_api_response(raw_data, company_details, "bo_details")
        
            self.data_list +=  parsed_data
            
            self.successful_company_query.add((company_details[0], company_details[1]))   
        
        except aiohttp.ClientConnectionError as e:
            self.failed_company_fetch.add(company_details)
            print(e)
            raise e
        except aiohttp.ClientError as e:
            print("Error with client sesson... closing")
            self.failed_company_fetch.add(company_details)
            raise e
        except Exception as e:
            print(e)
            self.failed_company_fetch.add(company_details)
    
    @concurrent_http_request_pool(concurrency=2)
    async def __fetch_for_co_name_wrapper(self, company_details, session=None)-> bool:
       
        # list of company details parsed
        try:
            
            raw_data = await self.__fetch_data_from_api(company_details, session, self.api_key, "co_name")
            
            parsed_data = self.__parse_api_response(raw_data, company_details, "co_name")
            
            # Remove company details from company queue
            self.company_queue.remove((company_details[0], company_details[1]))   
            
            # Replace with company name and company number
            self.company_queue.add((parsed_data[0], company_details[1]))
        
        except aiohttp.ClientConnectionError as e:
            self.failed_company_fetch.add(company_details)
            print(e)
            raise e
        except aiohttp.ClientError as e:
            print("Error with client sesson... closing")
            self.failed_company_fetch.add(company_details)
            raise e
        except Exception as e:
            print(e)
            self.failed_company_fetch.add(company_details)

    @concurrent_http_request_pool(concurrency=2)
    async def __fetch_charge_data_wrapper(self, company_details, session=None)-> bool:
        
        # list of company details parsed
        try:
            raw_data = await self.__fetch_data_from_api(company_details, session, self.api_key, "charge_data")
                        
            parsed_data = self.__parse_api_response(raw_data, company_details, "charge_details")
            
            for company in parsed_data:
                # Add company name and lis of parsed search results
                self.company_queue_post_name_fetch.add((company[0], company[1], company_details[0]))  # Company name, company number, company anem searched                
                
        
        except aiohttp.ClientConnectionError as e:
            self.failed_company_fetch.add(company_details)
            print(e)
            raise e
        
        except aiohttp.ClientError as e:
            print("Error with client sesson... closing")
            self.failed_company_fetch.add(company_details)
            raise e
            
        except Exception as e:
            print(e)
            self.failed_company_fetch.add(company_details)
    

    # Fetch data api
    @staticmethod
    async def __fetch_data_from_api(company_details, session: aiohttp.ClientSession, api_key, call_type) -> dict:
        
        if call_type == "psc":
            # The API response for PSC is found here https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/resources/list?v=latest
            url = f"https://api.company-information.service.gov.uk/company/{company_details[1]}/persons-with-significant-control"
        elif call_type == "charge_data":
            # Use company number
            url = f"https://api.company-information.service.gov.uk/company/{company_details[1]}/charges"
        elif call_type == "co_number":
            # Use CH search api using company name
            url = f"https://api.company-information.service.gov.uk/search/companies?q={company_details[0]}&items_per_page=2"
        elif call_type == "co_name":
            url = f"https://api.company-information.service.gov.uk/company/{company_details[1]}"

        
        time.sleep(0.6)
        # Basic auth setup
        auth = aiohttp.BasicAuth(api_key, "")
        
        try:
            async with session.get(url, auth=auth) as response:

                if response.status == 200:
                    return await response.json()
                else:
                    raise Exception(f"Could not fetch data - status code {response.status}")
    
        except aiohttp.ClientConnectionError as e:
            
            print(e)
            raise e
        except aiohttp.ClientError as e:
            
            print(e)
            raise e
    
        except Exception as e:
            raise e
    
    @staticmethod
    def __parse_api_response(api_response: dict, company_details: Tuple[str, str], parse_schema) -> List[dict]:
        
        results = []
        
        if parse_schema == "charge_details":
            results = charge_data_parse_schema(api_response, company_details)
        elif parse_schema == "bo_details":
            results = beneficial_owner_parse_schema(api_response, company_details)
        elif parse_schema == "co_name":
            results = company_name_parse_schema(api_response, company_details)
        elif parse_schema == "co_number":
            results = company_number_parse_schema(api_response, company_details)
        
        return results
    
    # Utility Methods
    @classmethod
    def __save_to_csv(cls, data_list, file_path):
        
        template_df = cls.df.copy()
        # Convert list of dicts to dataframe
        api_data_df = pd.DataFrame(data_list)
    
        file_exists = os.path.isfile()
        
        # Push data to dataframe
        new_df = pd.concat([template_df, api_data_df], ignore_index=True)
    
    
        # Write DataFrame to CSV
        with cls.lock:
            
            try:
                new_df.to_csv(file_path, mode='a', header=not file_exists, index=False)
            except Exception as e:
                print(f"Error writing to file: {e}")
    
    @classmethod 
    def __write_finished_queries(cls, successful_company_query, file_path) -> None:
        # Update company finished retriveing list
        with cls.lock:
            try:
                with open(file_path, "a", encoding="utf-8") as file:
                    for company in successful_company_query:
                        file.write(f"{company[0]}: {company[1]}\n")
            except Exception as e:
                print(f"Error writing to file: {e}")
    
    @classmethod
    def __write_unsuccessful_queries(cls, failed_company_fetch, file_path) -> None:
        
        # Update company finished retriveing list
        with cls.lock:
            with open(file_path, "a", encoding="utf-8") as file:
                for company in failed_company_fetch:
                    file.write(f"{company[0]}: {company[1]}\n")
    
    
    def __company_no_in_finished_list(self, company_number) -> bool:
        
        try:
            company_number = str(company_number)
        except:
            pass
        
        if not len(self.prev_successful_queries_list) >  0:
            with open(self.successful_query_output_file, "r", encoding="utf-8") as file:
                lines = file.readlines()
                
                for line in lines:
                    line = line.strip()  # Remove any leading/trailing whitespace characters
                    
                    # Split the line at the colon
                    _, number = line.split(':', 1)  # Split into two parts
                    number = number.strip()  # Remove extra spaces
                    
                    # Pad the number with leading zeros to ensure it is at least 8 digits long
                    # If no number exists, then don't pad it
                    if len(number) < 8 and len(number) > 0:
                        number = number.zfill(8)
                        
                    self.prev_successful_queries_list.add(number)
        
        if len(company_number) < 8 and len(company_number) > 0 :
            company_number = company_number.zfill(8)
                
        if company_number in self.prev_successful_queries_list:
            return True
        else:
            return False
     
    def __remove_company_details_from_unsuccessful_file(self, company_details, list_indicator):
        
        if list_indicator == "list":
            # Open the file to read and modify
            with open(self.failed_query_output_file, "r") as file:
                # Read all lines into a list
                lines = file.readlines()
            
            company_names = {company[0] for company in company_details}
        
            lines = [line for line in lines if not any(company in line for company in company_names)]
            
            # Write the modified lines back to the file
            with open(self.failed_query_output_file, "w") as file:
                file.writelines(lines)
        
        else:
        
            # Open the file to read and modify
            with open(self.failed_query_output_file, "r") as file:
                # Read all lines into a list
                lines = file.readlines()

            # Filter out the specific line
            lines = [line for line in lines if company_details[0] not in line]

            # Write the modified lines back to the file
            with open(self.failed_query_output_file, "w") as file:
                file.writelines(lines)