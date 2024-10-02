import asyncio
from functools import wraps
import aiohttp
import sys

async def __worker(self, task_func, queue: asyncio.Queue, session: aiohttp.ClientSession):
    
    while not queue.empty():
        item = await queue.get()
        try:
            # Call a process, such as a http request
            await task_func(self, item, session)
        except aiohttp.ClientConnectionError as e:
                raise e
        except aiohttp.ClientError as e:
                raise e
        finally:
            queue.task_done()

def concurrent_http_request_pool(concurrency=2):
    def decorator(task_func):
        @wraps(task_func)
        async def wrapper(self, item_list=set(), *args, **kwargs):
            
            # Define queue of query items for CompaniesHouse API calls
            queue = asyncio.Queue()
            
            # Populate queue with query items, such as company numbers
            for item in item_list:
                await queue.put(item)
            
            
            async with aiohttp.ClientSession() as session:
    
                # Create a pool of worker tasks for both API keys
                tasks = [
                    asyncio.create_task(__worker(self, task_func, queue, session))
                    for _ in range(concurrency)
                ]
                
                # Wait until all tasks can no longer process any more query items
                await queue.join()
                
                # Cancel all workers once the queue is empty
                for task in tasks:
                    task.cancel()
                
                # Ensure all tasks are completed and cleaned up
                await asyncio.gather(*tasks, return_exceptions=True)
        return wrapper
    return decorator
            
            