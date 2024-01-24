from flask import Flask,jsonify,redirect,request,url_for
import os
import subprocess
import requests

app = Flask(__name__)

All_servers = {}
DOCKER_IMAGE_NAME = "flaskserver"
DOCKER_API_VERSION = "3.9"

#For starting a new server
def start_new_server(server_id):

    container_name = f'container{server_id}'
    image_name = DOCKER_IMAGE_NAME

    res = os.popen(f'sudo docker run --name {container_name} --network ds_project_net1 --network-alias {container_name} -d {image_name}').read()

    All_servers[server_id] = container_name   #adding it to the list

    if len(res) > 0:
        print("Success")
    else:
        raise Exception("Failed to start the server. Check logs for details.")


@app.route('/<path>',methods=["GET"])
def path_redirect(path):    
    
    container_name = 'container7'
    id = '106'
    if path == 'home':
        response = requests.get(f"http://{container_name}:5000/home/{id}")
        return jsonify(response.json())
    elif path == 'heartbeat':
        response = requests.get(f"http://{container_name}:5000/heartbeat")
        return response.text, response.status_code
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
                server_id = int(hostnames[i][1:])
                start_new_server(server_id)
            return jsonify({'message': f'Successfully added {n} servers'}), 200
        else:
            return jsonify({'message': 'Invalid request, missing or incorrect parameters'}), 400
    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500


if __name__ == '__main__':
    app.run(debug = True,host = '0.0.0.0',port = 5000)
