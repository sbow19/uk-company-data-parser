from typing import Optional, Set, Tuple, List, Literal
from config import config

def company_name_parse_schema(api_response, company_details) -> Tuple[str, str]:
    new_details = (api_response["company_name"], company_details[1])
        
    # Return the parsed data 
    return new_details

def company_number_parse_schema(api_response, company_details) -> List[Tuple[str, str]]:
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

def charge_data_parse_schema(api_response, company_details) -> List[dict]:
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

def beneficial_owner_parse_schema(api_response, company_details) -> List[dict]:
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