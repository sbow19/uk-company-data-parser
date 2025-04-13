# CL arguments for the Companies House (--ch) option.        
#     1)Beneficiaries of charges over UK companies owning UK property - arg = charge
#     2)Owners of UK companies owning UK property - arg = dom
#     3)Owners of overseas companies owning UK property = for
CHARGS = ["charge", "dom", "for"]

# CL arguments for the HM Land Registry (--hmlr) option.        
#     1) Parse domestic companies with property dataset - arg = dom
#     2) Parse overseas companies with property dataset = for
HMLRARGS = ["both", "dom", "for"]


# Realtive path to diretcory containing HMLR datatsets
DATA_FILE_PATH = "./datasets"

# Location of list of already parsed files
PARSED_FILE_PATH = "./datasets/parsed_files.txt"

# Used to prevent duplicate API calls
UNIQUE_UK_COMPANIES_LIST_PATH = "./datasets/unique_UK_cos.txt"
UNIQUE_FOREIGN_COMPANIES_LIST_PATH = "./datasets/unique_overseas_cos.txt"
UNIQUE_COMPANIES_LIST_BUFFER_PATH = "./datasets/unique_buffer.txt"

# API REQUEST OUTPUT FILES 
UK_OWNER_DATA_OUTPUT_FILE = "./output/uk_owner_data.csv"
FOR_OWNER_DATA_OUTPUT_FILE = "./output/overseas_owner_data.csv"
CHARGE_DATA_OUTPUT_FILE = "./output/charge_data.csv"

CHARGE_DATA_FINISHED_QUERIES_PATH = "./output/charge_fin_queries.txt"
UK_OWNER_DATA_FINISHED_QUERIES_PATH = "./output/uk_co_queries.txt"
FOR_OWNER_DATA_FINISHED_QUERIES_PATH = "./output/overseas_co_queries.txt"

CHARGE_DATA_UNSUCCESSFUL_QUERIES_PATH="./output/charge_failed_queries.txt"
FOR_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH="./output/for_failed_queries.txt"
UK_OWNER_DATA_UNSUCCESSFUL_QUERIES_PATH="./output/uk_failed_queries.txt"

UK_COMPANY_CHARGE_HEADER=[
    "target_company_number",
    "target_company_name",
    "created_on",
    "person_entitled",
    "resolved_on",
    "satisfied_on",
    "status",
    
    "charge_number",
    "chargor_acting_as_bare_trustee",
    "contains_fixed_charge",
    "contains_floating_charge",
    "contains_negative_pledge",
    "description"
]
COMPANY_BO_HEADER=[
    "target_company_name",
    "target_company_number",
    "name",
    "kind",
    "ceased",
    "ceased_on",
    "notified_on",
    "is_sanctioned",
    
    "country_registered",
    "legal_authority",
    "place_registered",
    "registration_number",
    
    "nationality",
    "date_of_birth",
    "country_of_residence",
    
    "address"
]
