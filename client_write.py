import asyncio
import aiohttp
import matplotlib.pyplot as plt
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
 
 
async def main():
    write_link = 'http://127.0.0.1:5001/write'
    num_write_req = 100
    server_ids = []
    used_ids = set()
    start_time = time.time()
    timeout = aiohttp.ClientTimeout(total=10000)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []
 
        for i in range(num_write_req):
            stud_id = random.randint(0,16380)
            while stud_id in used_ids:
                stud_id = random.randint(0,16380)
            used_ids.add(stud_id)
 
        used_ids = list(used_ids)
 
        for i in range(num_write_req):
            write_payload = {
            "data": [{
                "Stud_id":used_ids[i],
                "Stud_name":''.join(random.choices(string.ascii_uppercase,k=8)),
                "Stud_marks": str(random.randint(0,100))
            }]
            }
            tasks.append(client_request(session, write_link, write_payload))
        responses = await asyncio.gather(*tasks)
        
        for res in responses:
            server_ids.append(json.loads(res)["message"].split()[-1])
    
    end_time = time.time()
    total_time = (end_time - start_time)
    speed = num_write_req/total_time
    # server_id_count = {server_id: server_ids.count(server_id) for server_id in set(server_ids)}
    print(f"Total time taken is {total_time} seconds.")
    print(f"Speed is {speed} requests per second.")
    # print(server_id_count)
    
 
if __name__ == '__main__':
    asyncio.run(main())