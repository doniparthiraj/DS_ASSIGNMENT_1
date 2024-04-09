from flask import Flask,jsonify,request
import os
from helper import SQLHandler
import threading
import requests
import json

# db_helper = SQLHandler()
logfile_info = {}
shard_locks = {}

app = Flask(__name__)

class ReadWriteLock:
    def __init__(self):
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0

    def acquire_write(self):
        """ Acquire a write lock. Blocks until there are no
        acquired read or write locks. """
        self._read_ready.acquire()
        while self._readers > 0:
            self._read_ready.wait()

    def release_write(self):
        """ Release a write lock. """
        self._read_ready.release()

@app.route('/home',methods=["GET"])
def home():
    get_id = request.args.get('id')
    response = {
        'message' : f'Hai from server {get_id}',
        'status' : 'successfull'
    }
    return jsonify(response), 200

@app.route('/heartbeat',methods=["GET"])
def heartbeat():
    response = {
        'message' : ' ',
        'code' : 200
    }
    return jsonify(response)


@app.route('/config', methods=['POST'])
def config():
    # data = request.json 
    db_helper = SQLHandler()
    server_name = request.args.get('id')
    print("config server",flush=True)
    db_helper.connect()
    try:
        request_payload = request.json
 
        # Validate the payload structure
        if 'schema' in request_payload and 'shards' in request_payload:
            for shard in request_payload['shards']:
                shard_locks[shard] = ReadWriteLock()
                key = server_name+"_"+shard 
                logfile_info[key] = 0
                print("logile info",logfile_info,flush=True)
                with open(f"{server_name}_{shard}.txt","w") as file:
                    pass
            response = db_helper.initialize_shard_tables(request_payload, server_name)
            return response
 
        return jsonify({"error": "Invalid payload structure"}), 400
 
    except Exception as e:
        print(e,flush=True)
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

@app.route('/write', methods=['POST'])
def write():
    print("inside write server",flush=True)
    # data = request.json 
    db_helper = SQLHandler()
    server_name = request.args.get('id')
    db_helper.connect()
    try:
        request_payload = request.json
        shard = request_payload['shard']
        servers = request_payload['servers']
        primary_server = request_payload['primary']
        request_payload['operation'] = 'write'
        print("request_payload",request_payload,flush=True)
        if request_payload['from'] == 'LB': #server is primary
            print("I amd primry server",flush=True)
            shard_locks[shard].acquire_write()
            try:
             
                with open(f"{server_name}_{shard}.txt", "a") as file:
                    logfile_info[f"{server_name}_{shard}"] = logfile_info[f"{server_name}_{shard}"]+1
                    file.write(f"{request_payload}$")
                count = 1
                for backup in servers:
                    if backup != primary_server:
                        print("I am a backup",flush=True)
                        try:
                            request_payload['from'] = primary_server
                            response = requests.post(f"http://{backup}:5000/write?id={backup}",json=request_payload)
                            data = json.loads(response.text)
                            print('write..........',data,flush=True)
                            # new_idx = data['message']['current_idx']
                            # db_mutex.acquire()
                            # try:
                            # update_response = db_helper.update_shard_idx(new_idx, shard_id)
                            # finally:
                                # db_mutex.release()
                            print('successfully updated idx in shard_T schema ')
                            if response.status_code == 200:
                                count = count+1
                                print("Request to", backup, "was successful")
                            else:
                                print("Request to", backup, "failed with status code:", response.status_code)
                        finally:
                            print("write to backup completed",flush=True)
            finally:
                shard_locks[shard].release_write()
            if count>((len(servers)-1)//2):
                print("majority replied",flush=True)
                if 'shard' in request_payload and 'data' in request_payload:
                    response = db_helper.write_to_database(request_payload)
                    return response
    
            return jsonify({"error": "Majority not reached or invalid payload structure"}), 400

        else:
            if request_payload['from'] != 'shard_manager':
                with open(f"{server_name}_{shard}.txt", "a") as file:
                    logfile_info[f"{server_name}_{shard}"] = logfile_info[f"{server_name}_{shard}"]+1
                    file.write(f"{request_payload}$")
                
        # Validate the payload structure
            if 'shard' in request_payload and 'data' in request_payload:
                response = db_helper.write_to_database(request_payload)
                print("Response in backup:",response,flush=True)
                return response
            return jsonify({"error": "Invalid payload structure"}), 400
 
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500
    
@app.route('/update', methods=['PUT'])
def update():
    # data = request.json 
    db_helper = SQLHandler()

    server_name = request.args.get('id')

    print("update server",flush=True)
    db_helper.connect()
    try:
        request_payload = request.json
        shard = request_payload['shard']
        servers = request_payload['servers']
        primary_server = request_payload['primary']
        request_payload['operation'] = 'update'
        print("request_payload",request_payload,flush=True)
        if request_payload['from'] == 'LB': #server is primary
            print("I am primry server",flush=True)
            shard_locks[shard].acquire_write()
            try:
             
                with open(f"{server_name}_{shard}.txt", "a") as file:
                    logfile_info[f"{server_name}_{shard}"] = logfile_info[f"{server_name}_{shard}"]+1
                    file.write(f"{request_payload}$")
                count = 1
                majority_count = 1
                for backup in servers:
                    if backup != primary_server:
                        print("I am a backup",flush=True)
                        try:
                            response = requests.get(f"http://{backup}:5000/heartbeat?id={backup}")
                            if response.status_code == 200:
                                majority_count += 1
                                request_payload['from'] = primary_server
                                response = requests.put(f"http://{backup}:5000/update?id={backup}",json=request_payload)
                                data = json.loads(response.text)
                                print('update..........',data,flush=True)
                                # new_idx = data['message']['current_idx']
                                # db_mutex.acquire()
                                # try:
                                # update_response = db_helper.update_shard_idx(new_idx, shard_id)
                                # finally:
                                    # db_mutex.release()
                                # print('successfully updated idx in shard_T schema ')
                                if response.status_code == 200:
                                    count = count+1
                                    print("Request to", backup, "was successful")
                                else:
                                    print("Request to", backup, "failed with status code:", response.status_code)
                        finally:
                            print("update to backup completed",flush=True)
            finally:
                shard_locks[shard].release_write()
            if count>(majority_count)//2:
                print("majority replied",flush=True)
                # Validate the payload structure
                if 'shard' in request_payload and 'Stud_id' in request_payload and 'data' in request_payload:
                    response = db_helper.update_to_database(request_payload)
                    return response

            return jsonify({"error": "Majority not reached or invalid payload structure"}), 400

        else:
            if request_payload['from'] != 'shard_manager':
                with open(f"{server_name}_{shard}.txt", "a") as file:
                    logfile_info[f"{server_name}_{shard}"] = logfile_info[f"{server_name}_{shard}"]+1
                    file.write(f"{request_payload}$")
                
        # Validate the payload structure

            if 'shard' in request_payload and 'Stud_id' in request_payload and 'data' in request_payload:
                    response = db_helper.update_to_database(request_payload)
                    return response
            return jsonify({"error": "Invalid payload structure"}), 400

 
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

    
@app.route('/delete', methods=['DELETE'])
def delete():
    # data = request.json 
    db_helper = SQLHandler()
    print("delete server",flush=True)
    db_helper.connect()
    try:
        request_payload = request.json
 
        # Validate the payload structure
        if 'shard' in request_payload and 'Stud_id' in request_payload:
            response = db_helper.delete_to_database(request_payload)
            return response
 
        return jsonify({"error": "Invalid payload structure"}), 400
 
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500
    

@app.route('/read', methods=['POST'])
def read():
    db_helper = SQLHandler()
    # data = request.json 
    print("read server",flush=True)
    db_helper.connect()
    try:
        request_payload = request.json
 
        # Validate the payload structure
        if 'shard' in request_payload and 'Stud_id' in request_payload:
            response = db_helper.read_from_database(request_payload)
            return response
 
        return jsonify({"error": "Invalid payload structure"}), 400
 
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

@app.route('/copy', methods=['GET'])
def copy():
    db_helper = SQLHandler()
    # data = request.json 
    print("copy server",flush=True)
    db_helper.connect()
    try:
        request_payload = request.json
 
        # Validate the payload structure
        if 'shards' in request_payload:
            response = db_helper.copy_from_database(request_payload)
            return response
 
        return jsonify({"error": "Invalid payload structure"}), 400
 
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

@app.route('/get_latest_index', methods=['POST'])
def get_latest_index():
    server_name = request.args.get('id')
    request_payload = request.json
    shard = request_payload['shard']
    ind = logfile_info[f"{server_name}_{shard}"]
    print("latest index",ind,flush=True)
    return jsonify({
        'latest_index': ind
        }), 200

@app.route('/get_log_entries', methods=['POST'])
def get_log_entries():
    server_name = request.args.get('id')
    request_payload = request.json
    shard = request_payload['shard']
    with open(f"{server_name}_{shard}.txt","r") as file:
        contents = file.read()
    print("contents of the log file",contents,flush=True)
    return jsonify({
        'contents': contents
        }), 200

@app.route('/write_log_entries',methods = ['POST'])
def write_log_entries():
    server_name = request.args.get('id')
    request_payload = request.json
    shard = request_payload['shard']
    contents = request_payload['contents']
    logfile_info[f"{server_name}_{shard}"] = len(contents.strip().split('$'))
    with open(f"{server_name}_{shard}.txt","a") as file:
        file.write(contents)
    print(f"Contents written to log file of {server_name}_{shard}",flush =True)
    return jsonify({'message':'Contents written successfully'}),200

if __name__ == '__main__':
    app.run(debug = True,host = '0.0.0.0',port = 5000)