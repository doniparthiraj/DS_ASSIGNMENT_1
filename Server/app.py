from flask import Flask,jsonify,request
import os

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

if __name__ == '__main__':
    app.run(debug = True,host = '0.0.0.0',port = 5000)