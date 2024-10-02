from tqdm.asyncio import tqdm_asyncio
import time
from functools import wraps

def time_progress(description, unit):
    def timed_progress_decorator(func):
        @wraps(func)
        async def wrapper(self, data_list=set(), pbar_position=0, *args, **kwargs):
            
            # Initialize the progress bar
            pbar = tqdm_asyncio(total=len(data_list), desc=description, unit=unit, position=pbar_position)

            # Start timing
            start_time = time.time()

            try:
                # Call the decorated function
                await func(self, data_list, pbar, *args, **kwargs)

                # Successfully completed
                print("Successfully scraped all companies")
            except Exception as e:
                print(e)
                raise Exception("Some error fetching data from Companies House... exiting program")
            finally:
                # Stop the progress bar
                pbar.close()

                # End timing
                end_time = time.time()
                elapsed_time = end_time - start_time
                print(f"Total time taken: {elapsed_time:.2f} seconds")

        return wrapper
    return timed_progress_decorator