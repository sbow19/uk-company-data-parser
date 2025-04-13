from database.db_connection import get_connection
from typing import Set, Tuple, List
from config import config
import shutil
import os
from lock.lock import create_lock, remove_process

def _uk_companies() -> Set[Tuple[int, int]]:
    connection = get_connection()
    cursor = connection.cursor()
    
    uk_companies = set()
    
    print("Fetching all companies from UK company records")
    
    # Get all unique company names from UK companies
    cursor.execute("""
        SELECT DISTINCT Proprietor_Name, Company_Registration_No FROM (
            SELECT Proprietor_Name_1 AS Proprietor_Name, Company_Registration_No_1 AS Company_Registration_No FROM hmlr.uk_companies
            UNION
            SELECT Proprietor_Name_2, Company_Registration_No_2 FROM hmlr.uk_companies
            UNION
            SELECT Proprietor_Name_3, Company_Registration_No_3 FROM hmlr.uk_companies
            UNION
            SELECT Proprietor_Name_4, Company_Registration_No_4 FROM hmlr.uk_companies
        ) AS combined
        """)
    firstUKSet: List[Tuple[str, str]] = cursor.fetchall()
    
    
    print("All companies fetched successfully.. processing...")
    
    for company in firstUKSet: 
        uk_companies.add(company) 
              
    
    print("Successfully fetched company records")
    
    return uk_companies

def _overseas_companies() -> Set[Tuple[int, int]]:
    connection = get_connection()
    cursor = connection.cursor()
    
    overseas_companies = set()
    print("Fetching all companies from overseas company records")
    
    # Get all unique company names from overseas companies
    cursor.execute("""
        SELECT DISTINCT Proprietor_Name, Company_Registration_No FROM (
            SELECT Proprietor_Name_1 AS Proprietor_Name, Company_Registration_No_1 AS Company_Registration_No FROM hmlr.overseas_companies
            UNION
            SELECT Proprietor_Name_2, Company_Registration_No_2 FROM hmlr.overseas_companies
            UNION
            SELECT Proprietor_Name_3, Company_Registration_No_3 FROM hmlr.overseas_companies
            UNION
            SELECT Proprietor_Name_4, Company_Registration_No_4 FROM hmlr.overseas_companies
        ) AS combined
        """)
    
    firstOverseasSet: List[Tuple[str, str]] = cursor.fetchall()
    
    print("All companies fetched successfully.. processing...")
    
    for company in firstOverseasSet:
        overseas_companies.add(company)
    
    print("Successfully fetched company records")
    
    return overseas_companies

def fetch_company_list_from_db(*args: str, update:bool=False) -> dict[str, Set[Tuple[int, int]]]:
    
    # Template response 
    company_list = {
        "uk_companies": set(),
        "overseas_companies": set()
    }
    

    if "uk" in args:
        create_lock("collect_uk_unique_list")
        loc = "uk"
        company_list["uk_companies"] = _uk_companies()
        __save_company_list_to_unique_company_list(company_list, config.UNIQUE_UK_COMPANIES_LIST_PATH, loc,  update)
        remove_process()
    
    if "overseas" in args:
        create_lock("collect_overseas_unique_list")
        loc = "overseas"
        company_list["overseas_companies"] = _overseas_companies()
        __save_company_list_to_unique_company_list(company_list, config.UNIQUE_FOREIGN_COMPANIES_LIST_PATH, loc, update)
        remove_process()
        
    return company_list
 
def __validate_unique_company_list_buffer_file() -> bool:
    with open(config.UNIQUE_COMPANIES_LIST_BUFFER_PATH) as file:
        for i, line in enumerate(file):
            if i > 1000:
                return True
        
        # If the file has less then 1000 lines, then the function returns false
        return False
    
# Compare company_list with existing unique company list, and add new companies if they don't appear
def __save_company_list_to_unique_company_list(company_list, file_path, loc, update):
     
    if loc == "uk":
        company_location = "uk_companies"
    else:
        company_location = "overseas_companies"
    
    # If update true, then we are rewriting over old file. 
    # We create a new buffer file to write into first so the old unique list isn't overwritten
    if not update:
        try:
            # Write company identifier list to company list file
            with open(file_path, "w", encoding="utf-8") as companies_list:
                for company in company_list[company_location]:
                    companies_list.write(f"{company[0]}: {company[1]}\n") 
            
            print("Successfully saved companies list to companies list file")
        except Exception as e:
            print(e)
            raise Exception("Some error saving companies list... ")  
    
    elif update: 
        
        try:
            # Write company identifier list to company list file
            with open(config.UNIQUE_COMPANIES_LIST_BUFFER_PATH, "w", encoding="utf-8") as companies_list:
                for company in company_list[company_location]:
                    companies_list.write(f"{company[0]}: {company[1]}\n") 
            
            # Once buffer is successfully saved, we perform a length check to see if there is a high chance that the companies were saved correctly
            if not __validate_unique_company_list_buffer_file():
                raise Exception("Some error validating unique company buffer list")
            
            # Now overwrite the existing unique company list file
            if loc == "uk":
                shutil.move(config.UNIQUE_COMPANIES_LIST_BUFFER_PATH, config.UNIQUE_UK_COMPANIES_LIST_PATH)
            elif loc == "overseas":
                shutil.move(config.UNIQUE_COMPANIES_LIST_BUFFER_PATH, config.UNIQUE_FOREIGN_COMPANIES_LIST_PATH)
            
        except Exception as e:
            print(e)
            raise Exception("Some error saving companies list... ")  
        
        finally:
            # Remove buffer
            
            os.remove(config.UNIQUE_COMPANIES_LIST_BUFFER_PATH)