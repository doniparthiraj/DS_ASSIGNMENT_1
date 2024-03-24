import asyncio
import aiohttp
import json
import random
import string
import time

async def client_request(session, url, payload=None):
    if payload:
        async with session.post(url, json=payload) as response:
            return await response.text()
    else:
        async with session.get(url) as response:
            return await response.text()

async def send_requests_in_batches(session, read_link, num_read_req, batch_size):
    tasks = []
    server_ids = []
    for _ in range(num_read_req // batch_size):
        batch_tasks = []
        for _ in range(batch_size):
            cli_id = random.randint(1, 10000)
            payload = {
                "Stud_id": {"low": random.randint(1, 8000), "high": random.randint(8001, 16000)}
            }
            url = f"{read_link}?id={cli_id}"
            batch_tasks.append(client_request(session, url, payload))
        responses = await asyncio.gather(*batch_tasks)
        for res in responses:
            server_ids.append(json.loads(res)["data"])
        await asyncio.sleep(0.1)  # Optional: add a short delay between batches
    return server_ids

async def main():
    read_link = 'http://127.0.0.1:5000/read'
    num_read_req = 10000
    batch_size = 100  # Adjust batch size based on your requirements
    timeout = aiohttp.ClientTimeout(total=500)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        start_time = time.time()
        server_ids = await send_requests_in_batches(session, read_link, num_read_req, batch_size)
        end_time = time.time()
    
    total_time = (end_time - start_time)
    speed = num_read_req / total_time
    server_id_count = {server_id: server_ids.count(server_id) for server_id in set(server_ids)}
    print(f"Total time taken is {total_time} seconds.")
    print(f"Speed is {speed} requests per second.")
    print(server_id_count)
    
if __name__ == '__main__':
    asyncio.run(main())
