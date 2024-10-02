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

class Scrape_UK_Company_Owners: 
    
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
    df = pd.DataFrame(columns=config.COMPANY_BO_HEADER)
    
    @classmethod
    def initialize_class(cls, lock):
        cls.lock = lock  # Set class-level lock

    @time_progress("Companies House UK Company PSC Scrape", "Company")
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
                    await self.__fetch_data_wrapper(self.company_queue)    # Company number is second object in tuple
                    # Save company data to csv

                    Scrape_UK_Company_Owners.__save_to_csv(self.data_list)
                    
                    # Update companies finished list file
                    Scrape_UK_Company_Owners.__write_finished_queries(self.successful_company_query)
                    
                    # Update list of failed queries
                    Scrape_UK_Company_Owners.__write_unsuccessful_queries(self.failed_company_fetch)
                    
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
                    
                    pbar.update(Scrape_UK_Company_Owners.chunk_size)
                    pbar.refresh()
                    continue
        
        try:
            await self.__fetch_data_wrapper(self.company_queue)    # Company number is second object in tuple
            # Save company data to csv

            Scrape_UK_Company_Owners.__save_to_csv(self.data_list)
            
            # Update companies finished list file
            Scrape_UK_Company_Owners.__write_finished_queries(self.successful_company_query)
            
            # Update list of failed queries
            Scrape_UK_Company_Owners.__write_unsuccessful_queries(self.failed_company_fetch)
            
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
            
            pbar.update(Scrape_UK_Company_Owners.chunk_size)
            pbar.refresh()
     
    @concurrent_http_request_pool(concurrency=2)
    async def __fetch_data_wrapper(self, company_details, session=None)-> bool:
       
        # list of company details parsed
        try:
            raw_data = await self.__fetch_data_from_api(company_details, session, self.api_key)
            
            parsed_data = self.__parse_api_response(raw_data, company_details)
        
            self.data_list +=  parsed_data
            
            self.successful_company_query.add(company_details)
        
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
        
    async def __fetch_data_from_api(self, company_details, session: aiohttp.ClientSession, api_key) -> dict:
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
    def __parse_api_response(api_response: dict, company_details: Tuple[str, str]) -> List[dict]:
        
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
           
    # Failed list scrape methods
    @time_progress("Companies House UK Company PSC Scrape", "Company")
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
            self.__remove_company_details_from_unsuccessful_file(self.company_queue_post_name_fetch_buffer, "list")
            
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
            with open(config.UK_OWNER_DATA_FINISHED_QUERIES_PATH, "r", encoding="utf-8") as file:
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
            with open(config.UK_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH, "r") as file:
                # Read all lines into a list
                lines = file.readlines()
            
            company_names = {company[0] for company in company_details}
        
            lines = [line for line in lines if not any(company in line for company in company_names)]
            
            # Write the modified lines back to the file
            with open(config.UK_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH, "w") as file:
                file.writelines(lines)
        
        else:
        
            # Open the file to read and modify
            with open(config.UK_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH, "r") as file:
                # Read all lines into a list
                lines = file.readlines()

            # Filter out the specific line
            lines = [line for line in lines if company_details[0] not in line]

            # Write the modified lines back to the file
            with open(config.UK_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH, "w") as file:
                file.writelines(lines)
            
   
    @classmethod
    def __save_to_csv(cls, data_list) :
        
        template_df = cls.df.copy()
        
        # Convert list of dicts to dataframe
        api_data_df = pd.DataFrame(data_list)
    
        file_exists = os.path.isfile(config.UK_OWNER_DATA_OUTPUT_FILE)
        
        # Push data to dataframe
        new_df = pd.concat([template_df, api_data_df], ignore_index=True)
    
        # Write DataFrame to CSV
        with cls.lock:
                
            try:
                new_df.to_csv(config.UK_OWNER_DATA_OUTPUT_FILE, mode='a', header=not file_exists, index=False)
            except Exception as e:
                print(f"Error writing to file: {e}")
        
    @classmethod
    def __write_finished_queries(cls, successful_company_query) -> None:
        
        with cls.lock:
            # Update company finished retriveing list
            with open(config.UK_OWNER_DATA_FINISHED_QUERIES_PATH, "a", encoding="utf-8") as file:
                for company in successful_company_query:
                    file.write(f"{company[0]}: {company[1]}\n")
            
            remove_dupes(config.UK_OWNER_DATA_FINISHED_QUERIES_PATH)
    
    @classmethod
    def __write_unsuccessful_queries(cls, failed_company_fetch) -> None:
        
        # Update company finished retriveing list
        
        with cls.lock:
            with open(config.UK_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH, "a", encoding="utf-8") as file:
                for company in failed_company_fetch:
                    file.write(f"{company[0]}: {company[1]}\n")
            
            remove_dupes(config.UK_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH)
   