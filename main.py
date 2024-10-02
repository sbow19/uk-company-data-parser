# This script creates a command-line wrapper around HMLR dataset ingestion
# and UK Companies House API call

import sys  # [System import]
from config import config
from ch_api.service import start_ch_service
from land_reg.service import start_hmlr_service
from win10toast import ToastNotifier
from lock.lock import remove_process
import asyncio


async def main() -> None:  # Entry point to programme

    # Check if arguments were passed
    if len(sys.argv) > 1:
        print(f"Arguments passed: {sys.argv[1:]}")
    else:
        print("Please enter an argument")
        raise Exception("Arguments required")
    
    # Reject if both ==ch and --hmlr were called
    if "--hmlr" in sys.argv and "--ch" in sys.argv:
        print("Only one operation can be performed at a time")
        return
    
    # Prints out the most recent
    if "--recent" in sys.argv:
        with open(config.RECENT_LOGS_PATH, "r", encoding="utf-8") as logs:
            for line in logs:
                print(line, end="")
        
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
    
    toast = ToastNotifier()
    try:
        asyncio.run(main())
        toast.show_toast("CLI Scrape", "Operation Successful", duration=2)
        remove_process()
        sys.exit(0)
    except Exception as e:
        toast.show_toast("CLI Scrape", "Operation Unsuccessful", duration=2)
        remove_process()
        sys.exit(1)
    
    
