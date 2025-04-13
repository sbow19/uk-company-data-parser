#This script imports lock functionality

import os
from typing import Literal, List

LOCKFILE = "tmp/processes.lock"
APILOCKFILE = "tmp/api_keys.lock"
Processes = ["ingest_csv", "fetch_uk_co_bo", "fetch_for_co_bo", "fetch_charge_data", "collect_uk_unique_list", "collect_overseas_unique_list"]


def create_lock(process_type) -> bool:
    #Create a lock file to indicate the program is running.
    if not os.path.exists(LOCKFILE):
        process_id = str(os.getpid())
        # Create the lock file
        with open(LOCKFILE, 'w') as lock:
            lock.write(f"{process_type}: {process_id}")  # Optionally write the process ID to the lock file
        
        # Return bool to indicate success
        return True
    
    else:
        # Check if process already underway
        with open(LOCKFILE, 'r') as lock:
            lines = lock.readlines()
        
        current_processes =  []
        for line in lines:
            current_process_type, _ = line.split(":", 1)
            current_processes.append(current_process_type)
            
        # Check if the lock file contains the current process type
        if process_type in current_processes:
            raise Exception(f"Process type {process_type} already running")
        else:
            # else, append to file
            process_id = str(os.getpid())
             # Create the lock file
            with open(LOCKFILE, 'a') as lock:
                lock.write(f"{process_type}: {process_id}")  # Optionally write the process ID to the lock file

def remove_process():
    
    if not os.path.exists(LOCKFILE):
        return False
    
    else:
        process_id = str(os.getpid())
        
        # get all lines
        with open(LOCKFILE, 'r') as lock:
            lines = lock.readlines()  
        
        # Filter out line with process type provided
        new_lines = [line for line in lines if process_id not in line]
        
        # If no processes, then delete lock file
        if len(new_lines) < 1:
            os.remove(LOCKFILE)
        else:
            # Save lock file
            with open(LOCKFILE, "w") as file: 
                file.writelines(new_lines)
        
    
def remove_lock():
    """Remove locl file when all processes are finished"""
    if os.path.exists(LOCKFILE):
        os.remove(LOCKFILE)


""" API KEY LOCKS to ensure proper distribution across application. MAX 2 keys per process """

def create_api_key_lock(process_type, api_key):
    if not os.path.exists(APILOCKFILE):
        
        # Create the lock file
        with open(APILOCKFILE, 'w') as lock:
            lock.write(f"{process_type}: {api_key}\n")  # Optionally write the process ID to the lock file
        
        # Return bool to indicate success
        return True
    else:
        # Check if process already underway
        with open(APILOCKFILE, 'r') as lock:
            lines = lock.readlines()
        
        current_api_keys =  []
        for line in lines:
            if not line.strip():
                continue
            _, current_api_key = line.split(":", 1)
            current_api_keys.append(current_api_key)
            
        # Check if the lock file contains the current process type
        if api_key in current_api_keys:
            raise Exception("API key already being utilised")
        else:
            # else, append to file
             # Create the lock file
            with open(APILOCKFILE, 'a') as lock:
                lock.write(f"{process_type}: {api_key}\n")  # Optionally write the process ID to the lock file

def fetch_current_api_keys() -> List:
    if os.path.exists(APILOCKFILE) :
        with open(APILOCKFILE, 'r') as lock:
            lines = lock.readlines()
        
        current_api_keys =  set()
        for line in lines:
            if not line.strip():
                continue
            _, current_api_key = line.split(":", 1)
            current_api_keys.add(current_api_key.strip())
        
        return current_api_keys
    else:
        return []

def get_available_api_keys()-> List:
    current_api_keys = fetch_current_api_keys()
    
    APIKEYS = [
    os.getenv("CH_API_KEY"),
    os.getenv("CH_API_KEY_2"),
    os.getenv("CH_API_KEY_3")
    ]
        
    available_api_keys = [key for key in APIKEYS if key not in current_api_keys]
    
    return available_api_keys
    
def remove_api_key_from_lock(api_key):
    if not os.path.exists(APILOCKFILE):
        return False
    
    else:
        # get all lines
        with open(APILOCKFILE, 'r') as lock:
            lines = lock.readlines()  
        
        # Filter out line with process type provided
        new_lines = [line for line in lines if api_key not in line]
        
        # Save lock file
        with open(APILOCKFILE, "w") as file: 
            file.writelines(new_lines)
    