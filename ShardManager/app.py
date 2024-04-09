from flask import Flask,jsonify,request
import numpy as np
import os
import threading
import time
import requests
import json
import random 
MapT = []
app = Flask(__name__)
All_servers = {}
DOCKER_IMAGE_NAME = "flaskserver"
DOCKER_API_VERSION = "3.9"

def get_servers_of_shards(shard):
    l = []
    for tup in MapT:
        if shard in tup:
            l.append(tup[1])
    return l

def leader_election(old_primary,shard):
    servers_list = get_servers_of_shards(shard)
    max_index = 0
    new_primary = None
    for server in servers_list:
        if server!=old_primary:
            response = requests.post(f"http://{server}:5000/get_latest_index?id={server}",json={"shard":shard})
            print("shard managesr response",response,flush=True)
            data = json.loads(response.text)
            ind = data.get('latest_index')
            print("latest_index",ind,flush=True)
            if ind > max_index:
                max_index = ind
                new_primary = server
    print("returning new primary")
    return new_primary

def generateId():
    while True:
        random_id = random.randint(1000, 9999)
        if random_id not in All_servers.values():
            return random_id

def start_new_server(ser_name):
    container_name = ser_name 
    image_name = DOCKER_IMAGE_NAME
    res = os.popen(f'sudo docker run --name {container_name} --network ds_assignment_1_net1 --network-alias {container_name} -d {image_name}').read()

    if len(res) > 0:
        All_servers[ser_name] = generateId() #adding it to the dict
        print("Success")
    else:
        raise Exception("Failed to start the server. Check logs for details.")

def add_oldshards(ser_name,shard_name,primary_server):
    #go and ask primary_server about the shard and get log file
    # execute the log file here
    try:
        print("inside add_old_shards",ser_name,shard_name,primary_server,flush=True)
        payload = {'shard':shard_name,'server':ser_name,'value':All_servers[ser_name]}
        res = requests.post(f"http://lb_server:5000/spawn_add_map_table",json = payload)
        res1 = requests.post(f"http://{primary_server}:5000/get_log_entries?id={primary_server}",json = payload)
        data = json.loads(res1.text)
        print(data,flush=True)

        data['shard'] = shard_name
        res2 = requests.post(f"http://{ser_name}:5000/write_log_entries?id={ser_name}",json=data)
        print("3",flush=True)
        response = requests.get(f"http://{primary_server}:5000/copy?id={primary_server}",json=payload)
        write_response = json.loads(response.text)
        print("4",flush=True)
        entries = write_response.get(shard_name)
        info = {
                'shard': shard_name,
                'data': entries,
                'from':'shard_manager',
                'primary':primary_server,
                'servers':[]
            }
        print("5",flush=True)
        response = requests.post(f"http://{ser_name}:5000/write?id={ser_name}",json=info)

    except Exception as e:
        print("Error in add_old_shard",str(e),flush=True)


def spawn_new_server(ser_name):
    global MapT
    try:
        data = {'server':ser_name}
        time.sleep(10)
        res = requests.post(f"http://lb_server:5000/remove_server_from_db",json=data)
        All_servers.pop(ser_name)
        #do leader election if ser_name is primary of any of the shards.
        all_shards = []
        for shard_ser in MapT:
            if shard_ser[1] == ser_name:
                all_shards.append([shard_ser[0],shard_ser[2]])
        
        for shard_ser in all_shards:
            if shard_ser[1] == 1:
                #do leader election and rewrite the MapT to the new primary_server of the shard.
                new_primary = leader_election(ser_name,shard_ser[0])
                MapT.append([shard_ser[0],new_primary,1])
                update_payload = {
                    'shard':shard_ser[0],
                    'server':new_primary
                }
                res = requests.post(f"http://lb_server:5000/update_map_table",json = update_payload)

        MapT = [shard_ser for shard_ser in MapT if shard_ser[1] != ser_name]
        random_id = random.randint(1,10)
        new_server = f'{ser_name}_{random_id}'
        start_new_server(new_server)
        db_schema = {
            'schema':{
                "columns":["Stud_id","Stud_name","Stud_marks"],
                "dtypes":["Number","String","String"]
                },
            'shards':[shard[0] for shard in all_shards]
        }
        time.sleep(10)
        res3 = requests.post(f"http://{new_server}:5000/config?id={new_server}",json=db_schema)
        time.sleep(2)
        for shard in all_shards:
            for shard_ser in MapT:
                if shard_ser[0] == shard[0] and shard_ser[2] == 1:
                    print("calling add_old_shards",new_server,shard[0],shard_ser[1],flush=True)
                    add_oldshards(new_server,shard[0],shard_ser[1])

    except Exception as e:
        print(f"Error: {e}")


def continuous_server_check():
    while(True):
        time.sleep(80)
        if bool(All_servers) == False:
            time.sleep(50)
            continue
        for ser_name in list(All_servers.keys()):
            if checkHeartbeat(ser_name) != 200:
                spawn_new_server(ser_name)
                print("server not found from shardmanager", ser_name, flush=True)
                time.sleep(20)
            else:
                print("server is there from shard mangaer",ser_name, flush=True)
        time.sleep(2)

server_check_thread = threading.Thread(target=continuous_server_check)
server_check_thread.daemon = True  # Daemonize the thread so it will exit when the main thread exits
server_check_thread.start()
        
def checkHeartbeat(server_name):
    try:
        response = requests.get(f"http://{server_name}:5000/heartbeat?id={All_servers[server_name]}")
        if response.status_code == 200:
            # Check if the response indicates no changes (304)
            return 304 if response.headers.get('content-length') == '0' else 200
        else:
            return response.status_code
    except Exception as e:
        print(f"Error: {e}")
        return 404

@app.route('/shardinit',methods=["POST"])
def init():
    global All_servers
    try:
        data = request.json
        primary = data['primary']
        servers = data['servers']
        All_servers = data['All_servers']
        for key,val in servers.items():
            for v in val:
                if v in primary and primary[v] == key:
                    MapT.append([v,key,1])
                else:
                    MapT.append([v,key,0])
        # print("Allservers",All_servers,flush=True)
        print("Inside ShardManager app.py",MapT,flush=True)
        return jsonify("Init Shard Manager Successful"),200
    except Exception as e:
        print('Shard Manager error in shardinit: ',e,flush=True)
        return jsonify({'message':f'Error :{str(e)}'}),500

@app.route('/shardrm',methods = ["POST"])
def shardrm():
    global All_servers
    global MapT
    try:
        data = request.json
        servers = data['servers']
        for ser in servers:
            All_servers.pop(ser)
        for ser in servers:
            primary_shards = [shard_ser[0] for shard_ser in MapT if shard_ser[1] == ser and shard_ser[2] == 1]
            for shard in primary_shards:
                new_primary = leader_election(ser,shard)
                MapT.append([shard,new_primary,1])
                update_payload = {
                    'shard':shard,
                    'server':new_primary
                }
                res = requests.post(f"http://lb_server:5000/update_map_table",json = update_payload)

            MapT = [shard_ser for shard_ser in MapT if shard_ser[1] != ser]
        return jsonify("Shard rm successful"),200
    except Exception as e:
        print('Shard Manager error in shardrm : ',e,flush=True)
        return jsonify({'message':f'Error :{str(e)}'}),500


if __name__ == '__main__':
    app.run(debug = True,host = '0.0.0.0',port = 5000)