import pandas as pd
from config import config
from ch_api.utils.progress import time_progress
from typing import Optional, Set, Tuple, List
from tqdm.asyncio import tqdm_asyncio
from config import config
from ch_api.utils.async_http import concurrent_http_request_pool
from ch_api.utils.remove_dupes import remove_dupes
import aiohttp
import time
import os

class Scrape_CH_Charges: 
    
    def __init__(self, api_key):
        self.data_list = []
        self.company_queue = set()
        self.company_name_fetch_queue: Set[Tuple[str, str]] = set()
        self.company_queue_post_name_fetch_buffer: Set[Tuple[str, str, str]] = set()
        self.successful_company_query = set()
        self.failed_company_fetch = set()
        self.api_key = api_key
        self.successful_numbers_list = set()
    
    # Class variables
    chunk_size = 100
    lock = None
    df = pd.DataFrame(columns=config.UK_COMPANY_CHARGE_HEADER)
    
    @classmethod
    def initialize_class(cls, lock):
        cls.lock = lock  # Set class-level lock
        
    @time_progress("Companies House Charge Scrape", "Company")
    async def scrape_data(self, company_details_list=set(), pbar: Optional[tqdm_asyncio]=None):
    
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
                    await self.__fetch_data_wrapper(self.company_queue) 
                    # Save company data to csv
                    Scrape_CH_Charges.__save_to_csv(self.data_list)
                    
                    # Update companies finished list file
                    Scrape_CH_Charges.__write_finished_queries(self.successful_company_query)
                    
                    # Update list of failed queries
                    Scrape_CH_Charges.__write_unsuccessful_queries(self.failed_company_fetch)
                    
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
                    
                    pbar.update(Scrape_CH_Charges.chunk_size)
                    pbar.refresh()
                    continue
        
        try:
                await self.__fetch_data_wrapper(self.company_queue)    # Company number is second object in tuple
                # Save company data to csv

                Scrape_CH_Charges.__save_to_csv(self.data_list)
                
                # Update companies finished list file
                Scrape_CH_Charges.__write_finished_queries(self.successful_company_query)
                
                # Update list of failed queries
                Scrape_CH_Charges.__write_unsuccessful_queries(self.failed_company_fetch)
                
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
            
            pbar.update(Scrape_CH_Charges.chunk_size)
            pbar.refresh()
            
    @concurrent_http_request_pool(concurrency=2)
    async def __fetch_data_wrapper(self, company_details, session=None)-> bool:
       
        # list of company details parsed
        try:
            raw_data = await self.__fetch_data_from_api(company_details, session, self.api_key)
            
            parsed_data = self.__parse_api_response(raw_data, company_details)
        
            self.data_list +=  parsed_data
            
            self.successful_company_query.add(company_details)
        
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
        
    async def __fetch_data_from_api(self, company_details, session: aiohttp.ClientSession, api_key) -> dict:
        # Replace with your actual API endpoint and parameters
        # The API response for PSC is found here https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/resources/list?v=latest
        url = f"https://api.company-information.service.gov.uk/company/{company_details[1]}/charges"
        
        time.sleep(0.6)
        # Basic auth setup
        auth = aiohttp.BasicAuth(api_key, "")
        
        try:
            async with session.get(url, auth=auth) as response:

                if response.status == 200:
                    return await response.json()
                else:
                    print(f"Could not fetch data - status code {response.status}")
                    raise Exception(f"Could not fetch data - status code {response.status}")
    
        except aiohttp.ClientConnectionError as e:
            
            print(e)
            raise e
        except aiohttp.ClientError as e:
            
            print(e)
            raise e
    
        except Exception as e:
            print(e)
            raise e
        
    @staticmethod
    def __parse_api_response(api_response: dict, company_details: Tuple[str, str]) -> List[dict]:
        
        # Create new dictonary with headers from api response 
        # Data container is desired api content listed as a dict
        data_container: List[dict] = []
    
        if api_response['total_count'] == 0 or len(api_response["items"]) == 0:
            return []
        
        # Charge list is returned as a list of dicts
        for charge in api_response["items"]:
        
            # We want a record for each person entitled to the charge 
            persons_entitled_list = charge["persons_entitled"]
            
            for person in persons_entitled_list:
        
                person_charge_details = {}
                
                for header in config.UK_COMPANY_CHARGE_HEADER:
                    if header == "target_company_name":
                        person_charge_details["target_company_name"] = company_details[0]
                    elif header == "target_company_number":
                        person_charge_details["target_company_number"] = company_details[1]
                    elif header == "chargor_acting_as_bare_trustee":
                        person_charge_details["chargor_acting_as_bare_trustee"] = charge["particulars"].get("chargor_acting_as_bare_trustee", None)
                    elif header == "contains_fixed_charge":
                        person_charge_details["contains_fixed_charge"] = charge["particulars"].get("contains_fixed_charge", None)
                    elif header == "contains_floating_charge":
                        person_charge_details["contains_floating_charge"] = charge["particulars"].get("contains_floating_charge", None)
                    elif header == "contains_negative_pledge":
                        person_charge_details["contains_negative_pledge"] = charge["particulars"].get("contains_negative_pledge", None)
                    elif header == "description":
                        person_charge_details["description"] = charge["particulars"].get("description", None)
                    elif header == "person_entitled":
                        person_charge_details["person_entitled"] = person["name"]
                    else:
                        person_charge_details[header] = charge.get(header, "")  
    
                data_container.append(person_charge_details) 
        # Return the parsed data 
        return data_container
              
   # Failed list scrape methods
    @time_progress("Companies House Failed UK Company Charge Scrape", "Company")
    async def scrape_failed_data(self, failed_company_list=set(), pbar: Optional[tqdm_asyncio]=None):
        
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
                self.company_queue_post_name_fetch_buffer.add(company_details)
            
            if len(self.company_queue_post_name_fetch_buffer) > self.chunk_size:
                
                remove_list = []
                for company in self.company_queue_post_name_fetch_buffer:
                        
                        # Check if company with number already successfully fetched
                        if self.__company_no_in_finished_list(company[1]):   
                            
                            remove_list.append(company_details)
                            self.company_queue_post_name_fetch_buffer.discard(company_details)
                        
                # Remove from failed details file
                self.__remove_company_details_from_unsuccessful_file(remove_list, "list")
                                
                await self.scrape_data(self.company_queue_post_name_fetch_buffer)
                
                self.__remove_company_details_from_unsuccessful_file(self.company_name_fetch_queue, "list")
                # Empty post name fetch buffer  
                self.company_queue_post_name_fetch_buffer = set()
                self.company_name_fetch_queue = set()
        
            
        # If buffer reaches certain length, then we scrape the data in the company queue
        if len(self.company_name_fetch_queue) > 0:
            
            try:
                # Attempt to get company number by CH search function
                await self.__fetch_co_number_data_wrapper(self.company_name_fetch_queue)
            except Exception as e:
                pass  
            
        # If buffer reaches certain length, then we scrape the data in the company queue
        if len(self.company_queue_post_name_fetch_buffer) > 0:
            
            remove_list = []
            for company in self.company_queue_post_name_fetch_buffer:
                    
                    # Check if company with number already successfully fetched
                    if self.__company_no_in_finished_list(company[1]):   
                        
                        remove_list.append(company_details)
                        self.company_queue_post_name_fetch_buffer.discard(company_details)
                    
            # Remove from failed details file
            self.__remove_company_details_from_unsuccessful_file(remove_list, "list")    
            
            # Scrape data
            await self.scrape_data(self.company_queue_post_name_fetch_buffer)
            
            # Finally remove companies in post name fetch queue
            self.__remove_company_details_from_unsuccessful_file(self.company_name_fetch_queue, "list")
            # Empty post name fetch buffer  
            self.company_queue_post_name_fetch_buffer = set()
            self.company_name_fetch_queue = set()
        
    @concurrent_http_request_pool(concurrency=2)
    async def __fetch_co_number_data_wrapper(self, company_details, session=None)-> bool:
       
        # list of company details parsed
        try:
            raw_data = await self.__fetch_co_number_data_from_api(company_details, session, self.api_key)

            parsed_data = self.__parse_co_number_api_response(raw_data)
            
            for company in parsed_data:
                # Add company name and lis of parsed search results
                self.company_queue_post_name_fetch_buffer.add((company[0], company[1]))  # Company name, company number, company anem searched                
        except aiohttp.ClientError as e:
            print("Error with client sesson... closing")
            self.failed_company_fetch.add(company_details)
            raise e
        except aiohttp.ClientConnectionError as e:
            self.failed_company_fetch.add(company_details)
            print(e)
            raise e
            
        except Exception as e:
            print(e)
            self.failed_company_fetch.add(company_details)
        
        return True
    
    async def __fetch_co_number_data_from_api(self, company_details, session: aiohttp.ClientSession, api_key) -> dict:
        # Replace with your actual API endpoint and parameters
        # The API response for copmany name search is found here https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/resources/companysearch?v=latest
        # Returns the top five search results
        url = f"https://api.company-information.service.gov.uk/search/companies?q={company_details[0]}&items_per_page=2"
        
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
    def __parse_co_number_api_response(api_response: dict) -> List[Tuple[str, str]]:
        # Create list of company names and numbers fetch from the api
        # Data container is desired api content listed as a dict
        data_container: List[Tuple[str, str]] = []

    
        # Where there were no results for the name, then we return no results
        if api_response['total_results'] == 0 or len(api_response["items"]) == 0:
            return []
        
        # Results list is returned as a list of dicts
        for search_result in api_response["items"]:
    
            data_container.append((search_result["title"], search_result["company_number"]))
            
            
        # Return the parsed data 
        return data_container
      
    def __company_no_in_finished_list(self, company_number) -> bool:
        
        try:
            company_number = str(company_number)
        except:
            pass
        
        if not len(self.successful_numbers_list) >  0:
            with open(config.CHARGE_DATA_FINISHED_QUERIES_PATH, "r", encoding="utf-8") as file:
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
                        
                    self.successful_numbers_list.add(number)
        
        if len(company_number) < 8 and len(company_number) > 0 :
            company_number = company_number.zfill(8)
                
        if company_number in self.successful_numbers_list:
            return True
        else:
            return False
        
    @staticmethod
    def __remove_company_details_from_unsuccessful_file(company_details, list_indicator):
        
        if list_indicator == "list":
            # Open the file to read and modify
            with open(config.CHARGE_DATA_UNSUCCESSFUL_QUERIES_PATH, "r") as file:
                # Read all lines into a list
                lines = file.readlines()
            
            company_names = {company[0] for company in company_details}
        
            lines = [line for line in lines if not any(company in line for company in company_names)]
            
            # Write the modified lines back to the file
            with open(config.CHARGE_DATA_UNSUCCESSFUL_QUERIES_PATH, "w") as file:
                file.writelines(lines)
        
        else:
        
            # Open the file to read and modify
            with open(config.CHARGE_DATA_UNSUCCESSFUL_QUERIES_PATH, "r") as file:
                # Read all lines into a list
                lines = file.readlines()

            # Filter out the specific line
            lines = [line for line in lines if company_details[0] not in line]

            # Write the modified lines back to the file
            with open(config.CHARGE_DATA_UNSUCCESSFUL_QUERIES_PATH, "w") as file:
                file.writelines(lines)

    @classmethod
    def __save_to_csv(cls, data_list) :
        
        template_df = cls.df.copy()
        
        # Convert list of dicts to dataframe
        api_data_df = pd.DataFrame(data_list)
    
        file_exists = os.path.isfile(config.CHARGE_DATA_OUTPUT_FILE)
        
        # Push data to dataframe
        new_df = pd.concat([template_df, api_data_df], ignore_index=True)
    
        # Write DataFrame to CSV
        new_df.to_csv(config.CHARGE_DATA_OUTPUT_FILE, mode='a', header=not file_exists, index=False)
          
    @classmethod
    def __write_finished_queries(cls, successful_company_query) -> None:
        
        # Update company finished retriveing list
        with open(config.CHARGE_DATA_FINISHED_QUERIES_PATH, "a", encoding="utf-8") as file:
            for company in successful_company_query:
                file.write(f"{company[0]}: {company[1]}\n")
    
    @classmethod
    def __write_unsuccessful_queries(cls, failed_company_fetch) -> None:
        
        # Update company finished retriveing list
        with open(config.CHARGE_DATA_UNSUCCESSFUL_QUERIES_PATH, "a", encoding="utf-8") as file:
            for company in failed_company_fetch:
                file.write(f"{company[0]}: {company[1]}\n")
    
   
    
    