from flask import Flask,jsonify,redirect,request,url_for
import os
import requests
import random
from consistent_hash import ConsistentHash as CH
from threading import Thread
import time

from helper import SQLHandler


app = Flask(__name__)

db_helper = SQLHandler()
# shard_hash = {}

hash = CH()
All_servers = {}
DOCKER_IMAGE_NAME = "flaskserver"
DOCKER_API_VERSION = "3.9"


# def continuous_server_check():
#     while(True):
#         if bool(All_servers) == False:
#             time.sleep(10)
#             continue
#         for ser_name in list(All_servers.keys()):
#             if checkHeartbeat(ser_name) != 200:
#                 spawn_new_server(ser_name)
#         time.sleep(2)

# server_check_thread = Thread(target=continuous_server_check)
# server_check_thread.daemon = True  # Daemonize the thread so it will exit when the main thread exits
# server_check_thread.start()



def generateId():
    while True:
        random_id = random.randint(1000, 9999)
        if random_id not in All_servers.values():
            return random_id
def create_server_name():
    while True:
        rand_name = random.randint(1000, 9999)
        server_name = "S"+str(rand_name)
        if server_name not in All_servers:
            break
    return server_name

#For starting a new server
def start_new_server(server_name):
    #giving container name and adding a new container
    container_name = server_name 
    image_name = DOCKER_IMAGE_NAME
    res = os.popen(f'sudo docker run --name {container_name} --network ds_assignment_1_net1 --network-alias {container_name} -d {image_name}').read()

    if len(res) > 0:
        All_servers[server_name] = generateId() #adding it to the dict
        hash.add_server_hash(server_name, All_servers[server_name])
        print("Success")
    else:
        raise Exception("Failed to start the server. Check logs for details.")

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

def removeServer(server_name):
    hash.rem_server(All_servers[server_name])
    print("rmove",flush = True)
    res = db_helper.remove_server(server_name)
    All_servers.pop(server_name) #removing from dict
    res = os.system(f'sudo docker stop {server_name} && sudo docker rm {server_name}')
    if res > 0:
        return jsonify({"message" : f"{server_name} not found","status" : "failure"}),400

def spawn_new_server(server_name):
    hash.rem_server(All_servers[server_name])
    All_servers.pop(server_name) #removing from dict
    os.system(f'sudo docker rm {server_name}')
    random_id = random.randint(1,10)
    new_server = f'{server_name}_{random_id}'
    start_new_server(new_server)

#gets the server for client using hash 
def get_avail_serv(cli_id, max_attempts = 10):
    attempts = 0
    while attempts < max_attempts:
        get_ser_name = hash.reqhash(int(cli_id))
        if get_ser_name is not None:
            if checkHeartbeat(get_ser_name) == 200: #used to see the server is alive or not and returns if avaliable
                return get_ser_name
            else:
                time.sleep(2)
        attempts += 1

    raise Exception("No available servers after multiple attempts")

def add_servers(servers):
    try:
        for server_name in servers:
            start_new_server(server_name)
        return 'success'
    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500


@app.route('/<path>',methods=["GET"])
def path_redirect(path):    
    if path == 'home' or path == 'heartbeat':
        cli_id = request.args.get('id')
        server_name = get_avail_serv(cli_id)
        response = requests.get(f"http://{server_name}:5000/{path}?id={server_name}")
        return jsonify(response.json())
    elif path == 'rep':
        # print("rep")
        res = list(All_servers.keys())
        if len(res) > 0:
            return jsonify({
                'message': {
                    "N": len(res),  # Use len(res) to get the length of the 'res' list
                    "replicas": [server for server in res]
                },
                "status": "successful"
            }), 200
        else:
            return jsonify({'message': 'Failed to laod the replicas'}), 400

    else:
        response = {
            'message' : f'Error endpoint does not exists -- {path}',
            'status' : 'failure'
        }
        return jsonify(response), 400

@app.route('/add',methods=["POST"])
def add():
    try:
        data = request.json
        n = data.get('n')
        new_shards = data.get('new_shards',[])
        servers = data.get('servers',{})
        new_servers = {}
        name_check = lambda s: all(char.isalnum() or char == '_' for char in s)
        if len(servers) >= n:
            for key,val in servers.items():
                ser_name = key
                if not name_check(ser_name):
                    ser_name = create_server_name()
                start_new_server(ser_name)
                new_servers[ser_name] = val
    
            for shard in new_shards:
                res = db_helper.add_shard_table(shard['Stud_id_low'],shard['Shard_id'],shard['Shard_size'],shard['Stud_id_low'])
            
            for key,val in new_servers.items():
                for v in val:
                    res = db_helper.add_map_table(v,key)
            
            for ser in new_servers.keys():
                info = {
                    'schema':{
                        "columns":["Stud_id","Stud_name","Stud_marks"],
                        "dtypes":["Number","String","String"]
                        },
                    'shards':new_servers[ser]
                }
                
                print(ser,info,flush = True)
                time.sleep(7)
                response = requests.post(f"http://{ser}:5000/config?id={ser}",json=info)
                if response.status_code == 200:
                    print("Request to", ser, "was successful")
                else:
                    print("Request to", ser, "failed with status code:", response.status_code)
            
            message = f'Add {", ".join(new_servers.keys())}'
            return jsonify({
                'N' : len(All_servers),
                'message': message,
                'status':'successful'
            }),200
            
        else:
            return jsonify({'message':'<Error> Number of new servers (n) is greater than newly added instances',
            'status':'failure'}),400
        

    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500


@app.route('/rm',methods=["DELETE"])
def rm():
    try:
        data = request.json
        n = data.get('n')
        hostnames = data.get('servers',[])
        deleted_servers = []
        if n < len(hostnames):
            return jsonify({"message" : "<Error> Length of servers list is more than removable instances"}),400
        elif n >= len(hostnames):
            for server in hostnames:
                if server != 'lb_server':
                    removeServer(server)
                    deleted_servers.append(server)
                else:
                    return jsonify({"message" : "Permission Denied","status" : "failure"}),400
            for _ in range( n - len(hostnames) ):
                result = list(All_servers.keys())
                if len(result) > 0: #checking if containers are available or not for removing
                    random_server = result[0]
                    removeServer(random_server)
                    deleted_servers.append(random_server)

            return jsonify({
                'message': {
                    "N": len(All_servers),  
                    "servers": deleted_servers
                        }
                    }), 200

    except Exception as e:
        return jsonify({'message': f'rmError: {str(e)}'}), 500


@app.route('/init', methods=['POST'])
def init():
    try:
        data = request.json

        servers = data['servers'].keys()
        add_servers(servers)

        shard_all = [shard['Shard_id'] for shard in data['shards']]

        db_helper.initialize_shard_map_table(data)
        shards_present = []
        for ser in servers:
            info = {
                'schema' : data['schema'],
                'shards' : data['servers'][ser]
            }
            shards_present.extend(info['shards']) 
            print(ser,info,flush = True)
            time.sleep(7)
            response = requests.post(f"http://{ser}:5000/config?id={ser}", json=info)
            if response.status_code == 200:
                print("Request to", ser, "was successful")
            else:
                print("Request to", ser, "failed with status code:", response.status_code)

        shard_absent = [x for x in shard_all if x not in shards_present]
        print("shardabsents:",shard_absent,flush = True)
        if len(shard_absent) != 0 :
            #then we need to randomly allocate some servers for the shards.
            for shard in shard_absent:
                random_ser = random.choice(list(All_servers.keys()))
                server_info = {}
                server_info['schema'] = data['schema']
                server_info['shards'] = [shard]
                res = db_helper.add_map_table(shard,random_ser)
                print(res,flush = True)
                
                response = requests.post(f"http://{random_ser}:5000/config?id={random_ser}",json=server_info)

        return jsonify({
            'message' : "Configured Database"
        }),200
    except Exception as e:
        return jsonify({'message':f'Error :{str(e)}'}),500


@app.route('/status',methods = ['GET'])
def status():
    try:
        serv_status = {key: [] for key in All_servers.keys()}
        shards,servers = db_helper.get_status(serv_status)
        schema = {"columns":["Stud_id","Stud_name","Stud_marks"],
                  "dtypes":["Number","String","String"]}#checkcorrectmethd for schema
        return jsonify({
                'N':len(All_servers),
                'schema':schema,
                'shards':shards,
                'servers':servers

        }),200

    except Exception as e:
        return jsonify({'message':f'Error :{str(e)}'}),500


@app.route('/read',methods = ['POST'])
def read():
    try:
        data = request.json
        low = data['Stud_id']['low']
        high = data['Stud_id']['high']
        print(low,high,flush=True)
        shards_req = db_helper.shards_required(low,high)
        print(shards_req,flush=True)
        return ' ',200
    except Exception as e:
        return jsonify({'message':f'Error :{str(e)}'}),500

if __name__ == '__main__':
    app.run(debug = True,host = '0.0.0.0',port = 5000)

