
import os
from config import config
from typing import Set, Tuple, List, Literal

def _create_unique_company_list_file(companies_file_path):
    # Create an empty file if it does not exist
        with open(companies_file_path, 'w', encoding="utf-8"):            
            print("Company list file created")
        
def _create_finished_companies_list_file(finished_queries_file_path):
    # Create an empty file if it does not exist
        with open(finished_queries_file_path, 'w', encoding="utf-8"):            
            print("Finished list file created")
            
def _create_failed_company_queries_list_file(failed_queries_file_path):
# Create an empty file if it does not exist
    with open(failed_queries_file_path, 'w', encoding="utf-8"):            
        print("Failed list file created")

# Output file creator functions

def create_charge_data_output_files():
    # Companies file path contains a list of all unique UK company numbers that appear in the HMLR
    # UK company land ownership datasets. This list is used to query the UK Companies House API for
    # charge information.
    
    #   Create new output file if none exist
    if not os.path.exists(config.UNIQUE_UK_COMPANIES_LIST_PATH):
        _create_unique_company_list_file(config.UNIQUE_UK_COMPANIES_LIST_PATH)
    
    # Finished queries list contains of company numbers for which charge data was successfully fetched from
    # the UK Companies House API (regardless of whether there was charge data or not), and stored in a local
    # CSV output file.
   
    # Create new output file if none exists
    if not os.path.exists(config.CHARGE_DATA_FINISHED_QUERIES_PATH):
        _create_finished_companies_list_file(config.CHARGE_DATA_FINISHED_QUERIES_PATH)
        
    if not os.path.exists(config.CHARGE_DATA_UNSUCCESSFUL_QUERIES_PATH):
        _create_failed_company_queries_list_file(config.CHARGE_DATA_UNSUCCESSFUL_QUERIES_PATH)


def create_foreign_co_owners_data_output_files():
    # Companies file path contains a list of all unique foreign company details that appear in the HMLR
    # foreign company land ownership datasets. This list is used to query the UK Companies House API for
    # beneficial owner information.
    
    #  Create new unique company file if none exist
    if not os.path.exists(config.UNIQUE_FOREIGN_COMPANIES_LIST_PATH):
       _create_unique_company_list_file(config.UNIQUE_FOREIGN_COMPANIES_LIST_PATH)
    
    # Finished queries list contains of company details for which beneficial owner data was successfully fetched from
    # the UK Companies House API (regardless of whether there was beneficial owner data or not)
   
    # Create new output file if none exists
    if not os.path.exists(config.FOR_OWNER_DATA_FINISHED_QUERIES_PATH):
        _create_finished_companies_list_file(config.FOR_OWNER_DATA_FINISHED_QUERIES_PATH)
        
    # Failed queries list contains of company numbers for which beneficial owner data was successfully fetched from
    # the UK Companies House API (regardless of whether there was beneficial owner data or not)
   
    # Create new output file if none exists
    if not os.path.exists(config.FOR_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH):
        _create_failed_company_queries_list_file(config.FOR_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH)
    
def create_uk_co_owners_data_output_files():
    # Companies file path contains a list of all unique foreign company details that appear in the HMLR
    # uk company land ownership datasets. This list is used to query the UK Companies House API for
    # beneficial owner information.
    
    #   Create new unqiue companyu list file if none exist
    if not os.path.exists(config.UNIQUE_UK_COMPANIES_LIST_PATH):
        _create_unique_company_list_file(config.UNIQUE_UK_COMPANIES_LIST_PATH)
    
    # Finished queries list contains of company details for which beneficial owner data was successfully fetched from
    # the UK Companies House API (regardless of whether there was beneficial owner data or not)
   
    # Create new output file if none exists
    if not os.path.exists(config.UK_OWNER_DATA_FINISHED_QUERIES_PATH):
        _create_finished_companies_list_file(config.UK_OWNER_DATA_FINISHED_QUERIES_PATH)
        
    # Failed queries list contains of company numbers for which beneficial owner data was successfully fetched from
    # the UK Companies House API (regardless of whether there was beneficial owner data or not)
   
    # Create new output file if none exists
    if not os.path.exists(config.UK_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH):
        _create_failed_company_queries_list_file(config.UK_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH)    
     
def __parse_file(file_loc, process_type = ""):
    company_list = set()    
    
    with open(file_loc, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()  # Remove any leading/trailing whitespace characters
            if not line:
                continue         # Skip empty lines
            
            # Split the line at the colon
            if ':' in line:
                company, number = line.split(':', 1)  # Split into two parts
                company = company.strip()  # Remove extra spaces
                number = number.strip()  # Remove extra spaces
                
                # Pad the number with leading zeros to ensure it is at least 8 digits long
                # If no number exists, then don't pad it
                if len(number) < 8 and len(number) > 0:
                    number = number.zfill(8)
            
                # Only process name of company for foreign company parse
                if process_type == "for":
                    company_list.add((company, "none"))
                else:    
                    # Append to the list as a tuple
                    company_list.add((company, number))
            
            else:
                print(f"Warning: Line does not contain a colon: '{line}'")
    
    return company_list
    
def read_company_list_file(company_loc_indicator: str) -> Set[Tuple[str, str]]:

    unique_company_list_file_path = ""
    service_type_indicator = ""

    if company_loc_indicator == "dom":
        unique_company_list_file_path = config.UNIQUE_UK_COMPANIES_LIST_PATH
        service_type_indicator = "dom"
    elif company_loc_indicator == "for":
        
        unique_company_list_file_path = config.UNIQUE_FOREIGN_COMPANIES_LIST_PATH
        service_type_indicator = "for"
        
    try:
        company_list = __parse_file(unique_company_list_file_path, service_type_indicator)
    except IOError as e:
        print(f"Error opening file: {e}")

    # Return the populated set 
    return company_list
        
def trim_company_list_file(unique_company_list: set, service_type_indicator: str ) ->  Set[Tuple[str, str]]:
    
    finished_company_list_file_path = ""
    failed_company_file_path = ""
    
    if service_type_indicator == "dom":
        finished_company_list_file_path = config.UK_OWNER_DATA_FINISHED_QUERIES_PATH
        failed_company_file_path = config.UK_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH
    elif service_type_indicator == "for":
        finished_company_list_file_path = config.FOR_OWNER_DATA_FINISHED_QUERIES_PATH
        failed_company_file_path = config.FOR_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH
    elif service_type_indicator == "charge":
        finished_company_list_file_path = config.CHARGE_DATA_FINISHED_QUERIES_PATH
        failed_company_file_path = config.CHARGE_DATA_UNSUCCESSFUL_QUERIES_PATH

    # Parse finished company flie
    try:
        finished_company_set = __parse_file(finished_company_list_file_path, service_type_indicator)
        failed_company_set = __parse_file(failed_company_file_path, service_type_indicator)
        
    except IOError as e:
        print(f"Error opening file: {e}")
        
    # Parse failed company file
    
    # reduce copmlete unqiue company details list with parsed company
    unique_company_list.difference_update(finished_company_set)
    unique_company_list.difference_update(failed_company_set)
    
    # Return the trimmed unique compay set 
    return unique_company_list

def read_failed_company_list_file(scrape_type: Literal["charge", "uk_bo", "for_bo"])-> Set[Tuple[str, str]]:
    
    failed_company_file_path = ""
    
    if scrape_type == "charge":
        failed_company_file_path = config.CHARGE_DATA_UNSUCCESSFUL_QUERIES_PATH
    elif scrape_type == "uk_bo":
        failed_company_file_path = config.UK_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH
    elif scrape_type == "for_bo":
        failed_company_file_path = config.FOR_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH
        
    try: 
        company_list = __parse_file(failed_company_file_path)
    except IOError as e:
        print(f"Error opening file: {e}")
    
    # Return the populated set 
    return company_list

def fetch_missed_numbers_overseas_output() -> Set:
    
    missing_numbers_set = set()
    current_numbers = set()
    
    company_details = __parse_file(config.FOR_OWNER_DATA_FINISHED_QUERIES_PATH)
    
    if len(company_details) < 100: 
        i = 0
        while i < 35001:
            # Convert missed number to string
            missed_number_string = str(i)
            missed_number_string = missed_number_string.zfill(6)
                    
            missed_number_full_string = f"OE{missed_number_string}"
            missing_numbers_set.add(missed_number_full_string)
            
            i += 1
        
        return missing_numbers_set
            
    
    for company in company_details: 
        
        company_number: str = company[1]     # Number is a string value
        
        if ":" in company_number:
            _, company_number = company_number.split(':', 1)  # Split into two parts
            company_number = company_number.strip()  # Remove extra spaces
        
        upper_company_number = company_number.upper()
        
            
        if "OE" in upper_company_number:
            
            number_raw = upper_company_number[2:]

            number = int(number_raw)
            current_numbers.add(number)
    
    # Order the companies in set by company number,ascending
    current_numbers_sorted = sorted(current_numbers)
    
    # Find missing numbers by comparing diffence between x and x + 1 element in sorted array
    i = 0
    while i < len(current_numbers_sorted) - 1:
        
        difference = current_numbers_sorted[i + 1] - current_numbers_sorted[i]
        
        # If there is a greater gap than one, then at least one number is missing
        if difference > 1:
            for j in range(1, difference):
                
                # Convert missed number to string
                missed_number_string = str(current_numbers_sorted[i] + j)
                
                if len(missed_number_string) < 6:
                    missed_number_string = missed_number_string.zfill(6)
                
                missed_number_full_string = f"OE{missed_number_string}"
                
                missing_numbers_set.add(missed_number_full_string)
            
        i+=1
    
    
    return missing_numbers_set
        

def do_output_files_exist()-> List[str]:
    
    output_file_list = []
    
    if os.path.exists(config.UK_OWNER_DATA_OUTPUT_FILE):
        output_file_list.append(config.UK_OWNER_DATA_OUTPUT_FILE)
    if os.path.exists(config.FOR_OWNER_DATA_OUTPUT_FILE):
        output_file_list.append(config.FOR_OWNER_DATA_OUTPUT_FILE)
    if os.path.exists(config.CHARGE_DATA_OUTPUT_FILE):
        output_file_list.append(config.CHARGE_DATA_OUTPUT_FILE)
    
    return output_file_list

def does_failed_list_exist(service_type):
    
    if service_type == "charge":
        with open(config.CHARGE_DATA_UNSUCCESSFUL_QUERIES_PATH, "r", encoding="utf-8") as file:
            for i, line in enumerate(file):
                if i >= 10:  # We want at least 10 lines, so if we reach n-1, we have enough
                    return True
                
        return False
    
    elif service_type == "uk_co_bo":
        with open(config.UK_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH, "r", encoding="utf-8") as file:
            for i, line in enumerate(file):
                if i >= 10:  # We want at least 10 lines, so if we reach n-1, we have enough
                    return True
                
        return False
        
    elif service_type == "for_co_bo":
        with open(config.FOR_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH, "r", encoding="utf-8") as file:
            for i, line in enumerate(file):
                if i >= 10:  # We want at least 10 lines, so if we reach n-1, we have enough
                    return True
                
        return False
        
def does_finished_list_exist(service_type):
    
    if service_type == "charge":
        with open(config.CHARGE_DATA_FINISHED_QUERIES_PATH, "r", encoding="utf-8") as file:
            for i, line in enumerate(file):
                if i >= 10:  # We want at least 10 lines, so if we reach n-1, we have enough
                    return True
                
        return False
    
    elif service_type == "uk_co_bo":
        with open(config.UK_OWNER_DATA_FINISHED_QUERIES_PATH, "r", encoding="utf-8") as file:
            for i, line in enumerate(file):
                if i >= 10:  # We want at least 10 lines, so if we reach n-1, we have enough
                    return True
                
        return False
        
    elif service_type == "for_co_bo":
        with open(config.FOR_OWNER_DATA_FINISHED_QUERIES_PATH, "r", encoding="utf-8") as file:
            for i, line in enumerate(file):
                if i >= 10:  # We want at least 10 lines, so if we reach n-1, we have enough
                    return True
                
        return False