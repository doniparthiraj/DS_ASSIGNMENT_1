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
def start_new_server(server_id):

    container_name = f'container{server_id}'
    image_name = DOCKER_IMAGE_NAME

    res = os.popen(f'sudo docker run --name {container_name} --network ds_assignment_1_net1 --network-alias {container_name} -d {image_name}').read()

    if len(res) > 0:
        All_servers[server_id] = container_name
        hash.add_server_hash(server_id)
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
                server_id = hostnames[i]
                start_new_server(server_id)
            return jsonify({'message': f'Successfully added {n} servers'}), 200
        else:
            return jsonify({'message': 'Invalid request, missing or incorrect parameters'}), 400
    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500


if __name__ == '__main__':
    app.run(debug = True,host = '0.0.0.0',port = 5000)
