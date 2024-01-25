from flask import Flask,jsonify,redirect,request,url_for
import os
import subprocess
import requests
from consistent_hash import ConsistentHash as CH

app = Flask(__name__)

hash = CH()
All_servers = {}
DOCKER_IMAGE_NAME = "flaskserver"
DOCKER_API_VERSION = "3.9"


#For starting a new server
def start_new_server(server_name):

    container_name = server_name
    image_name = DOCKER_IMAGE_NAME

    res = os.popen(f'sudo docker run --name {container_name} --network ds_assignment_1_net1 --network-alias {container_name} -d {image_name}').read()

    
    if len(res) > 0:
        All_servers[server_name] = "container_"+server_name   #adding it to the dict
        hash.add_server_hash(server_name)

        print("Success")
    else:
        raise Exception("Failed to start the server. Check logs for details.")

def checkHeartbeat(server_id):
    response,code = requests.get(f"http://{All_servers[server_id]}:5000/heart?id={server_id}")
    return code

def get_avail_serv(cli_id):

    get_ser_id = hash.reqhash(cli_id)
    while True:
        if get_ser_id is None:
            raise Exception("No servers are available")
        if checkHeartbeat(get_ser_id) == 200:
            return get_ser_id
        else:
            #removecontainer call
            hash.server_failure(get_ser_id)

@app.route('/<path>',methods=["GET"])
def path_redirect(path):    
    if path == 'home':
        cli_id = request.args.get('id')
        server_id = get_avail_serv(cli_id)
        response = requests.get(f"http://{All_servers[server_id]}:5000/home?id={server_id}")

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
        if n is not None and hostnames is not None and isinstance(hostnames, list):
            for i in range(n):

                server_name = hostnames[i]
                start_new_server(server_name)

            return jsonify({'message': f'Successfully added {n} servers'}), 200
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
        elif n > len(hostnames):
            for server in hostnames:
                if server != 'lb_server':
                    res = os.system(f'sudo docker stop {server} && sudo docker rm {server}')
                    if res > 0:
                        return jsonify({"message" : f"{server} not found","status" : "failure"}),400
                    All_servers.pop(server) #removing from dict
                else:
                    return jsonify({"message" : "Permission Denied","status" : "failure"}),400

            #we need to randomly remove servers if n > len(hostnames)
            for _ in range(n-len(hostnames)):
                result = list(All_servers.keys())
                if len(result) > 0: #checking if containers are available or not for removing
                    random_server = result[0]
                    res = os.system(f'sudo docker stop {random_server} && sudo docker rm {random_server}')
                    All_servers.pop(random_server)
                    hostnames.append(random_server)

            return jsonify({
                'message': {
                    "N": n,  
                    "replicas": [server for server in hostnames]
                        },
                        "status": "successful"
                    }), 200

        else:
            for server in hostnames:
                if server != 'lb_server':
                    res = os.system(f'sudo docker stop {server} && sudo docker rm {server}')
                    print("resposnse", res, flush=True)
                    if res > 0:
                        return jsonify({"message" : f"{server} not found","status" : "failure"}),400
                    All_servers.pop(server) #removing from dict
                else:
                    return jsonify({"message" : "Permission Denied","status" : "failure"}),400

            return jsonify({
                'message': {
                            "N": n,  # Use len(res) to get the length of the 'res' list
                            "replicas": [server for server in hostnames]
                            },
                            "status": "successful"
                        }), 200
                

    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500



if __name__ == '__main__':
    app.run(debug = True,host = '0.0.0.0',port = 5000)