import asyncio
import aiohttp
import json
import random
import time

async def client_request(session, url, payload=None):
    if payload:
        async with session.post(url, json=payload) as response:
            return await response.text()
    else:
        async with session.get(url) as response:
            return await response.text()

async def send_requests(session, read_link, num_read_req):
    server_ids = []
    for _ in range(num_read_req):
        cli_id = random.randint(1, 100000)
        l = random.randint(1, 16000)
        h = random.randint(1, 16380)
        while(h < l):
            h = random.randint(1, 16380)
        payload = {
            "Stud_id": {"low": l, "high": h}
        }
        url = f"{read_link}?id={cli_id}"
        response_text = await client_request(session, url, payload)
        server_ids.append(json.loads(response_text)["data"])
        # Optional: add a short delay between requests if needed
        # await asyncio.sleep(0.01)
    return server_ids

async def main():
    read_link = 'http://127.0.0.1:5001/read'
    num_read_req = 100  # Total number of read requests
    timeout = aiohttp.ClientTimeout(total=10000)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        start_time = time.time()
        server_ids = await send_requests(session, read_link, num_read_req)
        end_time = time.time()

    total_time = end_time - start_time
    speed = num_read_req / total_time
    print(f"Total time taken is {total_time} seconds.")
    print(f"Speed is {speed} requests per second.")
    

if __name__ == '__main__':
    asyncio.run(main())
