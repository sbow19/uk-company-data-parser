import pandas as pd
from config import config
from ch_api.utils.progress import time_progress
from typing import Optional, Set, Tuple, List
from tqdm.asyncio import tqdm_asyncio
from config import config
from ch_api.utils.async_http import concurrent_http_request_pool
import aiohttp
import time
import os

class Scrape_Foreign_Company_Owners:
    
    def __init__(self, api_key):
        self.data_list = []
        self.company_queue = set()
        self.company_queue_post_name_fetch: Set[Tuple[str, str, str]] = set()
        self.successful_company_query = set()
        self.failed_company_fetch = set()
        self.api_key = api_key
    
    # Class variables
    chunk_size = 100
    lock = None
    df = pd.DataFrame(columns=config.COMPANY_BO_HEADER)
    
    @classmethod
    def initialize_class(cls, lock):
        cls.lock = lock  # Set class-level lock

    @time_progress("Overseas Companies Beneficial Owners Scrape", "Company")
    async def scrape_data(self, company_details_list=set(), pbar: Optional[tqdm_asyncio]=None):   
        
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

                    Scrape_Foreign_Company_Owners.__save_to_csv(self.data_list)
                    
                    # Update companies finished list file
                    Scrape_Foreign_Company_Owners.__write_finished_queries(self.successful_company_query)
                    
                    # Update list of failed queries
                    Scrape_Foreign_Company_Owners.__write_unsuccessful_queries(self.failed_company_fetch)
                    
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
                
                    pbar.update(Scrape_Foreign_Company_Owners.chunk_size)
                    pbar.refresh()
                    continue
           
    @time_progress("Missed Overseas Companies Beneficial Owners Scrape", "Company")
    async def scrape_missed_data(self, company_numbers_list=set(), pbar: Optional[tqdm_asyncio]=None):   
        
        
        for missed_number in company_numbers_list:
            
            missed_number = ("", missed_number)
            self.company_queue.add(missed_number)
            
            # Once the length of the chunk reaches 1000, then we provide the company ids to the fetch data method
            if len(self.company_queue) == self.chunk_size:
                
                try:
                    
                    await self.__fetch_for_co_name_wrapper(self.company_queue)
                    # Get
                    await self.__fetch_bo_data_wrapper(self.company_queue)
                    # Save company data to csv
                    Scrape_Foreign_Company_Owners.__save_to_csv(self.data_list)
                    
                    # Update companies finished list file
                    Scrape_Foreign_Company_Owners.__write_finished_queries(self.successful_company_query)
                    
                    # Update list of failed queries
                    Scrape_Foreign_Company_Owners.__write_unsuccessful_queries(self.failed_company_fetch)
                    
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
                
                    pbar.update(Scrape_Foreign_Company_Owners.chunk_size)
                    pbar.refresh()
                    continue
        
        # Clear out remaining numbers
        try:
    
            await self.__fetch_for_co_name_wrapper(self.company_queue)
    
            await self.__fetch_bo_data_wrapper(self.company_queue)
            # Save company data to csv

            Scrape_Foreign_Company_Owners.__save_to_csv(self.data_list)
            
            # Update companies finished list file
            Scrape_Foreign_Company_Owners.__write_finished_queries(self.successful_company_query)
            
            # Update list of failed queries
            Scrape_Foreign_Company_Owners.__write_unsuccessful_queries(self.failed_company_fetch)
                    
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
        
            pbar.update(Scrape_Foreign_Company_Owners.chunk_size)
            pbar.refresh()
           
    # Processes for fetching company numbers
    @concurrent_http_request_pool(concurrency=2)
    async def __fetch_co_number_data_wrapper(self, company_details, session=None)-> bool:
        
        # list of company details parsed
        try:
            raw_data = await self.__fetch_co_number_data_from_api(company_details, session, self.api_key)
                        
            parsed_data = self.__parse_co_number_api_response(raw_data)
            
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
    
    @concurrent_http_request_pool(concurrency=2)
    async def __fetch_bo_data_wrapper(self, company_details, session=None)-> bool:
       
        # list of company details parsed
        try:
            
            raw_data = await self.__fetch_bo_data_from_api(company_details, session, self.api_key)
            
            parsed_data = self.__parse_bo_api_response(raw_data, company_details)
        
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
    
    async def __fetch_bo_data_from_api(self, company_details, session: aiohttp.ClientSession, api_key) -> dict:
        # Replace with your actual API endpoint and parameters
        # The API response for PSC is found here https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/resources/list?v=latest
        url = f"https://api.company-information.service.gov.uk/company/{company_details[1]}/persons-with-significant-control"
        
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
    def __parse_bo_api_response(api_response: dict, company_details: Tuple[str, str, str]) -> List[dict]:
        
        # Create new dictonary with headers from api response 
        # Data container is desired api content listed as a dict
        data_container: List[dict] = []
    
        if api_response['total_results'] == 0 or len(api_response["items"]) == 0:
            return []
        
        # PSCs list is returned as a list of dicts
        for psc in api_response["items"]:
        
            psc_details = {}
            
            for header in config.COMPANY_BO_HEADER:
                if header == "target_company_name":
                    
                    if len(company_details[0]) < 1:
                        psc_details["target_company_name"] = ""
                    else:
                        psc_details["target_company_name"] = company_details[0]
                    
                    
                elif header == "target_company_number":
                    psc_details["target_company_number"] = company_details[1]
                    
                elif header == "country_registered":
                    
                    identification_obj = psc.get("identification", "")
                    
                    if identification_obj:
                        psc_details["country_registered"] = identification_obj.get(header, "")
                
                elif header == "legal_authority":
                    
                    identification_obj = psc.get("identification", "")
                    
                    if identification_obj:
                        psc_details["legal_authority"] = identification_obj.get(header, "")
                
                elif header == "place_registered":
                    
                    identification_obj = psc.get("identification", "")
                    
                    if identification_obj:
                        psc_details["place_registered"] = identification_obj.get(header, "")
                
                elif header == "date_of_birth":
                    
                    date_of_birth_obj = psc.get("date_of_birth", "")
                    
                    if date_of_birth_obj:
                        day = date_of_birth_obj.get("day", "")
                        month = date_of_birth_obj.get("month", "")
                        year = date_of_birth_obj.get("year", "")
                        psc_details["date_of_birth"] = f"{year}-{month}-{day}"
                        
                
                elif header == "address":
                    
                    address_obj = psc.get("address", "")
                    
                    if address_obj:
                        address_line_1 = address_obj.get("address_line_1", "")
                        address_line_2 = address_obj.get("address_line_2", "")
                        care_of = address_obj.get("care_of", "")
                        country = address_obj.get("country", "")
                        locality = address_obj.get("locality", "")
                        po_box = address_obj.get("po_box", "")
                        postal_code = address_obj.get("postal_code", "")
                        premises = address_obj.get("premises", "")
                        region = address_obj.get("region", "")
                        
                        psc_details["address"] = f"{address_line_1}, {address_line_2}, {care_of}, {country}, {locality}, {po_box}, {postal_code}, {premises}, {region}"
                    else:
                        psc_details["address"] = False
                     
                    
                else:
                    psc_details[header] = psc.get(header, "")  
            data_container.append(psc_details) 
        # Return the parsed data 
        return data_container
    
    
    @concurrent_http_request_pool(concurrency=2)
    async def __fetch_for_co_name_wrapper(self, company_details, session=None)-> bool:
       
        # list of company details parsed
        try:
            
            raw_data = await self.__fetch_for_co_name(company_details, session, self.api_key)
            
            parsed_data = self.__parse_for_co_name_response(raw_data, company_details)
            
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
    
    async def __fetch_for_co_name(self, company_details, session: aiohttp.ClientSession, api_key) -> dict:
        # Replace with your actual API endpoint and parameters
        # The API response for PSC is found here https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/resources/list?v=latest
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
    def __parse_for_co_name_response(api_response: dict, company_details: Tuple[str, str, str]) -> Tuple[str, str]:
        
        # Create new dictonary with headers from api response 
        # Data container is desired api content listed as a dict
        
        new_details = (api_response["company_name"], company_details[1])
        
        # Return the parsed data 
        return new_details
    
    
    @classmethod
    def __save_to_csv(cls, data_list):
        
        template_df = cls.df.copy()
        # Convert list of dicts to dataframe
        api_data_df = pd.DataFrame(data_list)
    
        file_exists = os.path.isfile(config.FOR_OWNER_DATA_OUTPUT_FILE)
        
        # Push data to dataframe
        new_df = pd.concat([template_df, api_data_df], ignore_index=True)
    
    
        # Write DataFrame to CSV
        with cls.lock:
            
            try:
                new_df.to_csv(config.FOR_OWNER_DATA_OUTPUT_FILE, mode='a', header=not file_exists, index=False)
            except Exception as e:
                print(f"Error writing to file: {e}")
            
    @classmethod
    def __write_finished_queries(cls, successful_company_query) -> None:
        # Update company finished retriveing list
        with cls.lock:
            try:
                with open(config.FOR_OWNER_DATA_FINISHED_QUERIES_PATH, "a", encoding="utf-8") as file:
                    for company in successful_company_query:
                        file.write(f"{company[0]}: {company[1]}\n")
            except Exception as e:
                print(f"Error writing to file: {e}")
    
    @classmethod
    def __write_unsuccessful_queries(cls, failed_company_fetch) -> None:
        
        # Update company finished retriveing list
        with cls.lock:
            with open(config.FOR_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH, "a", encoding="utf-8") as file:
                for company in failed_company_fetch:
                    file.write(f"{company[0]}: {company[1]}\n")