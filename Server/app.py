from flask import Flask,jsonify,request
import os
from helper import SQLHandler
import threading


sh1_lock = threading.Lock()
sh2_lock = threading.Lock()
sh3_lock = threading.Lock()
sh3_lock = threading.Lock()

shard_locks = {
    "sh1": sh1_lock,
    "sh2": sh2_lock,
    "sh3": sh3_lock,
    "sh4": sh4_lock
}

db_helper = SQLHandler()

app = Flask(__name__)

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
    print("config server",flush=True)
    db_helper.connect()
    try:
        request_payload = request.json
 
        # Validate the payload structure
        if 'schema' in request_payload and 'shards' in request_payload:
            response = db_helper.initialize_shard_tables(request_payload)
            return response
 
        return jsonify({"error": "Invalid payload structure"}), 400
 
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

@app.route('/write', methods=['POST'])
def write():
    # data = request.json 
    print("write server",flush=True)
    db_helper.connect()
    try:
        request_payload = request.json
 
        # Validate the payload structure
        if 'shard' in request_payload and 'curr_idx' in request_payload and 'data' in request_payload:
            shard_name = request_payload.get('shard', "\0")
            lock = shard_locks.get(shard_name)
            with lock:
                response = db_helper.write_to_database(request_payload)
                return response
 
        return jsonify({"error": "Invalid payload structure"}), 400
 
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500
    
@app.route('/update', methods=['POST'])
def update():
    # data = request.json 
    print("update server",flush=True)
    db_helper.connect()
    try:
        request_payload = request.json
 
        # Validate the payload structure
        if 'shard' in request_payload and 'Stud_id' in request_payload and 'data' in request_payload:
            shard_name = request_payload.get('shard', "\0")
            lock = shard_locks.get(shard_name)
            with lock:
                response = db_helper.update_to_database(request_payload)
                return response
 
        return jsonify({"error": "Invalid payload structure"}), 400
 
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500
    
@app.route('/delete', methods=['POST'])
def delete():
    # data = request.json 
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

@app.route('/copy', methods=['POST'])
def copy():
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


if __name__ == '__main__':
    app.run(debug = True,host = '0.0.0.0',port = 5000)