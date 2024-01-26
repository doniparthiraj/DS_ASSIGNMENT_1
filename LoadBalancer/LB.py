from flask import Flask,jsonify,redirect,request,url_for
import os
import subprocess
import requests
import random
from consistent_hash import ConsistentHash as CH

app = Flask(__name__)

hash = CH()
All_servers = {}
DOCKER_IMAGE_NAME = "flaskserver"
DOCKER_API_VERSION = "3.9"

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
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return 404

def removeServer(server_name):
    hash.rem_server(All_servers[server_name])
    All_servers.pop(server_name) #removing from dict
    res = os.system(f'sudo docker stop {server_name} && sudo docker rm {server_name}')
    if res > 0:
        return jsonify({"message" : f"{server} not found","status" : "failure"}),400

def spawn_new_server(server_name):
    hash.rem_server(All_servers[server_name])
    All_servers.pop(server_name) #removing from dict
    os.system(f'sudo docker rm {server_name}')
    random_id = random.randint(1,10)
    new_server = f'{server_name}_{random_id}'
    start_new_server(new_server)

def get_avail_serv(cli_id, max_attempts = 2):
    attempts = 0
    while attempts < max_attempts:
        get_ser_name = hash.reqhash(int(cli_id))
        if get_ser_name is not None:
            if checkHeartbeat(get_ser_name) == 200:
                return get_ser_name
            else:
                spawn_new_server(get_ser_name)
        attempts += 1

    raise Exception("No available servers after multiple attempts")


@app.route('/<path>',methods=["GET"])
def path_redirect(path):    
    if path == 'home':
        cli_id = request.args.get('id')
        server_name = get_avail_serv(cli_id)
        response = requests.get(f"http://{server_name}:5000/home?id={server_name}")
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
        data = request.get_json()
        n = data.get('n')
        hostnames = data.get('hostnames')
        if n < len(hostnames):
            return jsonify({"message" : "<Error> Length of hostname list is more than newly added instances","status" : "failure"}),400
        elif n is not None and hostnames is not None and isinstance(hostnames, list):
            for server_name in hostnames:
                start_new_server(server_name)
            #randomly add a server name if n > len(hostnames)
            length = len(hostnames)
            for _ in range(n-length):
                server_name = create_server_name()
                hostnames.append(server_name)
                start_new_server(server_name)

            return jsonify({
                'message': {
                    "N": n,  
                    "replicas": [server for server in hostnames]
                        },
                        "status": "successful"
                    }), 200 
        else:
            return jsonify({'message': 'Invalid request, missing or incorrect parameters'}), 400
    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500


@app.route('/rm',methods=["DELETE"])
def rm():
    try:
        data = request.get_json()
        n = data.get('n')
        hostnames = data.get('hostnames')
        if n < len(hostnames):
            return jsonify({"message" : "<Error> Length of hostname list is more than removable instances","status" : "failure"}),400
        elif n >= len(hostnames):
            for server in hostnames:
                if server != 'lb_server':
                    removeServer(server)
                else:
                    return jsonify({"message" : "Permission Denied","status" : "failure"}),400
            for _ in range( n - len(hostnames) ):
                result = list(All_servers.keys())
                if len(result) > 0: #checking if containers are available or not for removing
                    random_server = result[0]
                    removeServer(random_server)

            return jsonify({
                'message': {
                    "N": n,  
                    "replicas": [server for server in All_servers.keys()]
                        },
                        "status": "successful"
                    }), 200

    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500



if __name__ == '__main__':
    app.run(debug = True,host = '0.0.0.0',port = 5000)

