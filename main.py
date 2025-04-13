# This script creates a command-line wrapper around HMLR dataset ingestion and CH Companies House API calls
# In order to function, a .env must be added to the config directory containing all the credentials
# for API access and local mysql instance access.

import sys
from config import config
from ch_api.service import start_ch_service
from land_reg.service import start_hmlr_service
from lock.lock import remove_process
import asyncio
from dotenv import load_dotenv, dotenv_values
import os


async def main() -> None:  # Entry point to programme
    '''
        User has one of two services to choose via the command line: Companies House (--ch)
        or HM Land Registry (--hmlr).
        
        --hmlr:  Triggers the ingestion of HMLR datasets into the local mysql instance.
        --ch: triggers the scraping of the Companies House API,depending on other params
    '''
    
    load_dotenv(override=True)
    

    # Check if arguments were passed
    if len(sys.argv) > 1:
        print(f"Arguments passed: {sys.argv[1:]}")
    else:
        print("Please enter an argument")
        raise Exception("Arguments required")
    
    # Reject if both --ch and --hmlr were called
    if "--hmlr" in sys.argv and "--ch" in sys.argv:
        print("Only one operation can be performed at a time")
        return

        
    # Triggers Companies House API Calls.
    # Users can provide one argument
        # update streams:
        #     1)Beneficiaries of charges over UK companies owning UK property - arg = charge
        #     2)Owners of UK companies owning UK property - arg = dom
        #     3)Owners of overseas companies owning UK property = for
    if "--ch" in sys.argv:
        ch_index = sys.argv.index("--ch") + 1
        arg = sys.argv[ch_index]
        
        if arg in config.CHARGS:
            await start_ch_service(arg)
        return

    
    # Triggers ingesting of property datasets
    # Users can chose which datasets to parse
    if "--hmlr" in sys.argv:
        hmlr_index = sys.argv.index("--hmlr") + 1
        arg = sys.argv[hmlr_index]
        
        if arg in config.HMLRARGS:
            start_hmlr_service(arg)
            return



if __name__ == '__main__':
    
    try:
        # Async running due to API concurrency
        asyncio.run(main())
        remove_process()
        sys.exit(0)
    except Exception as e:
        remove_process()
        sys.exit(1)
    
    
