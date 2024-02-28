import asyncio
import aiohttp
import matplotlib.pyplot as plt
import json
import random

async def client_request(session,url):
    async with session.get(url) as response:
        return await response.text()


async def main():
    link = 'http://127.0.0.1:5000/home?id='

    num_req = 10000
    server_ids = []

    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(1,num_req+1):
            cli_id = random.randint(1,10000)
            url = link + str(cli_id)
            tasks.append(client_request(session,url))
        
        responses = await asyncio.gather(*tasks)
        
        for res in responses:
            server_ids.append(json.loads(res)["message"].split()[-1])
        
        
    
    server_id_count = {server_id: server_ids.count(server_id) for server_id in set(server_ids)}

    
    ids, counts = zip(*sorted(server_id_count.items()))

    plt.bar(ids, counts)
    plt.xlabel('Server ID')
    plt.ylabel('Number of Occurrences')
    plt.title('Number of Times Each Server ID Appeared')
    plt.savefig('server_id_plot.png')
    plt.show()



if __name__ == '__main__':
    asyncio.run(main())

            

