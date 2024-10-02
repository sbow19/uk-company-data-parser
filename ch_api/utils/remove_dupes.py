import os

def __parse_file(file_loc):
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
            
                # Append to the list as a tuple
                company_list.add((company, number))
            
            else:
                print(f"Warning: Line does not contain a colon: '{line}'")

    return company_list

def __create_file(file_loc, unique_company_list):
    
    with open(file_loc, "w", encoding="utf-8") as file:
        for company in unique_company_list:
            file.write(f"{company[0]}: {company[1]}\n")
        

def remove_dupes(file_loc):
    
    unique_company_list = __parse_file(file_loc)
    
    __create_file(file_loc, unique_company_list)
