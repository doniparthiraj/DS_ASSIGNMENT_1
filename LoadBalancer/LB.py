from flask import Flask,jsonify,redirect,request,url_for
import os
import subprocess
import requests

app = Flask(__name__)

All_servers = {}
DOCKER_IMAGE_NAME = "flaskserver"
DOCKER_API_VERSION = "3.9"

#For starting a new server
def start_new_server(server_name):

    container_name = server_name
    image_name = DOCKER_IMAGE_NAME

    res = os.popen(f'sudo docker run --name {container_name} --network ds_project_net1 --network-alias {container_name} -d {image_name}').read()

    All_servers[server_name] = server_name   #adding it to the list

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
    elif path == 'rep':
        # print("rep")
        res = rep()
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
        print("Else")
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


def rep():
    try:
        result = subprocess.run(['docker', 'ps', '--format', '{{.Names}}'], check=True, capture_output=True, text=True)

        # Split the output into a list of container names
        container_names = result.stdout.strip().split('\n')
        # print(container_names, flush=True)
        return container_names
        
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
                    print("resposnse", res, flush=True)
                    if res < 0:
                        return jsonify({"message" : f"{server} not found","status" : "failure"}),400
                else:
                    return jsonify({"message" : "Permission Denied","status" : "failure"}),400
            
            for _ in range(n-len(hostnames)):
                result = subprocess.run(['docker', 'ps', '--format', '{{.Names}}'], check=True, capture_output=True, text=True)
                random_server = result.stdout.strip().split('\n')[0]
                if random_server == 'lb_server':
                    random_server = result.stdout.strip().split('\n')[1]
                res = os.system(f'sudo docker stop {random_server} && sudo docker rm {random_server}')
                
                if res > 0:
                    return jsonify({"message" : f"{random_server} not found","status" : "failure"}),400
                hostnames.append(random_server)
            return jsonify({
                'message': {
                    "N": n,  # Use len(res) to get the length of the 'res' list
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
                    return jsonify({
                        'message': {
                                    "N": n,  # Use len(res) to get the length of the 'res' list
                                    "replicas": [server for server in hostnames]
                                },
                                "status": "successful"
                            }), 200
                else:
                    return jsonify({"message" : "Permission Denied","status" : "failure"}),400

    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500



if __name__ == '__main__':
    app.run(debug = True,host = '0.0.0.0',port = 5000)

