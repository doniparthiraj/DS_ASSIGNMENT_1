from flask import Flask,jsonify,request
import os
from helper import SQLHandler

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
    data = request.json 
    db_helper.connect()
    try:
        request_payload = request.json
        print(request_payload,flush = True)
        # Validate the payload structure
        if 'schema' in request_payload and 'shards' in request_payload:
            response = db_helper.initialize_shard_tables(request_payload)
            return jsonify(response)
 
        return jsonify({"error": "Invalid payload structure"}), 400
 
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500
    return jsonify({"message": "Data received successfully"})

if __name__ == '__main__':
    app.run(debug = True,host = '0.0.0.0',port = 5000)