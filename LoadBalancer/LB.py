from flask import Flask,jsonify,redirect,request,url_for
import os
import requests
import random
from consistent_hash import ConsistentHash as CH
import threading
import time
import json
from helper import SQLHandler
from queue import Queue

app = Flask(__name__)

db_helper = SQLHandler()
shard_hash = {}
shard_locks = {}
server_locks = {}

db_mutex = threading.Lock()

hash = CH()
All_servers = {}
DOCKER_IMAGE_NAME = "flaskserver"
DOCKER_API_VERSION = "3.9"


def continuous_server_check():
    while(True):
        time.sleep(80)
        if bool(All_servers) == False:
            time.sleep(50)
            continue
        for ser_name in list(All_servers.keys()):
            if checkHeartbeat(ser_name) != 200:
                spawn_new_server(ser_name)
                time.sleep(9)
        time.sleep(2)

server_check_thread = threading.Thread(target=continuous_server_check)
server_check_thread.daemon = True  # Daemonize the thread so it will exit when the main thread exits
server_check_thread.start()

class ReadWriteLock:
    def __init__(self):
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0

    def acquire_read(self):
        """ Acquire a read lock. Blocks only if a thread has
        acquired the write lock. """
        self._read_ready.acquire()
        try:
            self._readers += 1
        finally:
            self._read_ready.release()

    def release_read(self):
        """ Release a read lock. """
        self._read_ready.acquire()
        try:
            self._readers -= 1
            if not self._readers:
                self._read_ready.notifyAll()
        finally:
            self._read_ready.release()

    def acquire_write(self):
        """ Acquire a write lock. Blocks until there are no
        acquired read or write locks. """
        self._read_ready.acquire()
        while self._readers > 0:
            self._read_ready.wait()

    def release_write(self):
        """ Release a write lock. """
        self._read_ready.release()

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
    try:
        hash.rem_server(All_servers[server_name])
        remove_server_from_shards(server_name)
        res = db_helper.remove_server(server_name)
        All_servers.pop(server_name) #removing from dict
        res = os.system(f'sudo docker stop {server_name} && sudo docker rm {server_name}')
        if res > 0:
            return jsonify({"message" : f"{server_name} not found","status" : "failure"}),400
    except Exception as e:
        print('removeServer: Main server remov error: ',e,flush=True)
        return jsonify({'message': f'Error: {str(e)}'}), 500

def remove_server_from_shards(server_name):
    try:
        shard_ser = db_helper.all_shard_servers()
        for shard, servers in shard_ser.items():
            if server_name in servers:
                servers.remove(server_name)
                shard_hash[shard].rem_server(All_servers[server_name])
                print(f"Removed {server_name} from {shard}",flush=True)
    except Exception as e:
        print('remov serever from shard error',e,flush=True)
        return jsonify({'message': f'Error: {str(e)}'}), 500


def spawn_new_server(server_name):
    shards = db_helper.get_shards_server(server_name)
    hash.rem_server(All_servers[server_name])
    remove_server_from_shards(server_name)
    res = db_helper.remove_server(server_name)
    server_locks.pop(server_name)
    All_servers.pop(server_name) #removing from dict
    os.system(f'sudo docker rm {server_name}')
    random_id = random.randint(1,10)
    new_server = f'{server_name}_{random_id}'
    start_new_server(new_server)
    data = { new_server : shards}
    add_oldshards([], data, True)


#gets the server for client using hash 
def get_avail_serv(cli_id, hash_obj, max_attempts = 10):
    attempts = 0
    while attempts < max_attempts:
        get_ser_name = hash_obj.reqhash(int(cli_id))
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
    except Exception as e:
        print('Error while adding servers: ',e,flush=True)
        return jsonify({'message': f'Error: {str(e)}'}), 500

def add_oldshards(newshard_ids, new_servers, spawning = False):
    if spawning:
        time.sleep(9)
        
        x = list(new_servers.keys())
        s_name = x[0]
        for key,val in new_servers.items():
            for v in val:
                res = db_helper.add_map_table(v,key)
        db_schema = {
            'schema':{
                "columns":["Stud_id","Stud_name","Stud_marks"],
                "dtypes":["Number","String","String"]
                },
            'shards':new_servers[s_name]
        }
        response = requests.post(f"http://{s_name}:5000/config?id={s_name}",json=db_schema)
        if response.status_code == 200:
            print("Request to", s_name, "was successful")
        else:
            print("Request to", s_name, "failed with status code:", response.status_code)

        for x in new_servers[s_name]:
            shard_hash[x].add_server_hash(s_name, All_servers[s_name])

        server_locks[s_name] = ReadWriteLock()

    for new_ser, shards in new_servers.items():
        for sid in shards:
            if sid not in newshard_ids:
                ser = db_helper.get_shard_servers(sid)
                print('1',ser,flush=True)
                server_name = ser[0]
                print('2',server_name,flush=True)
                try:
                    info = {'shards':[sid]}
                    print('3',info,flush=True)
                    response = requests.get(f"http://{server_name}:5000/copy?id={server_name}",json=info)
                    data = json.loads(response.text)
                    print('4',data,flush=True)
                    entries = data.get(sid)
                    print('5',entries,flush=True)
                    get_idx = db_helper.get_lowid(sid)
                    print('6',get_idx,flush=True)
                    info = {
                        'shard': sid,
                        'curr_idx': get_idx,
                        'data': entries
                    }
                    if len(data) == 0:
                        print('No data is present in shard',flush=True)
                        return '',200
                    response = requests.post(f"http://{new_ser}:5000/write?id={new_ser}",json=info)
                    data = json.loads(response.text)
                    print('write..........',data,flush=True)
                    # new_idx = data['message']['current_idx']
                    # update_response = db_helper.update_shard_idx(new_idx, shard_id)
                    # print('successfully updated idx in shard_T schema ')
                    if response.status_code == 200:
                        print("Request to", new_ser, "was successful")
                    else:
                        print("Request to", new_ser, "failed with status code:", response.status_code)
                except Exception as e:
                    print('Error while adding old shards',e,flush=True)
                    return jsonify({'message': f'Error: {str(e)}'}), 500 

def read_to_shard(shard_id, server_name, low, high, read_queue):

        shard_locks[shard_id].acquire_read()
        info = {
            "shard" : shard_id,
            "Stud_id":{'low' : low, 'high' : high}
        }
        try:
            response = requests.post(f"http://{server_name}:5000/read?id={server_name}",json=info)
            data = json.loads(response.text)
            #print('Inside',data,flush=True)
            read_queue.put(data)
        except Exception as e:
            print('Error while reading from shards in read_to_shard func: ',e,flush=True)
            return jsonify({'message': f'Error: {str(e)}'}), 500
        finally:
            shard_locks[shard_id].release_read()


def write_to_shard(shard_id, entries):

    shard_locks[shard_id].acquire_write()
    
    try:
        db_mutex.acquire()
        try:
            get_servers = db_helper.get_shard_servers(shard_id)
            get_idx = db_helper.get_shard_idx(shard_id)
        finally:
            db_mutex.release()
        info = {
            'shard': shard_id,
            'curr_idx': get_idx,
            'data': entries
        }
        # print(info,flush=True)
        # print(shard_id,get_servers,flush=True)
        for ser in get_servers:
            server_locks[ser].acquire_write()
            try:
                response = requests.post(f"http://{ser}:5000/write?id={ser}",json=info)
                data = json.loads(response.text)
                # print('write..........',data,flush=True)
                new_idx = data['message']['current_idx']
                db_mutex.acquire()
                try:
                    update_response = db_helper.update_shard_idx(new_idx, shard_id)
                finally:
                    db_mutex.release()
                # print('successfully updated idx in shard_T schema ')
                if response.status_code == 200:
                    print("Request to", ser, "was successful")
                else:
                    print("Request to", ser, "failed with status code:", response.status_code)
            finally:
                server_locks[ser].release_write()
    except Exception as e:
        print('Error while writing to shards in write_to_shard func: ',e,flush=True)
        return jsonify({'message': f'Error: {str(e)}'}), 500
    finally:
        shard_locks[shard_id].release_write()

@app.route('/<path>',methods=["GET"])
def path_redirect(path):    
    if path == 'home' or path == 'heartbeat':
        cli_id = request.args.get('id')
        server_name = get_avail_serv(cli_id, hash)
        response = requests.get(f"http://{server_name}:5000/{path}?id={server_name}")
        return jsonify(response.json())
    elif path == 'rep':
        res = list(All_servers.keys())
        if len(res) > 0:
            info = {
                    'message': {
                        "N": len(res),
                        "replicas": [server for server in res]
                    },
                    "status": "successful"
            }
            return jsonify(info), 200
        else:
            return jsonify({'message': 'Failed to laod the replicas'}), 400

    else:
        response = {
            'message' : f'Error endpoint does not exists -- {path}',
            'status' : 'failure'
        }
        return jsonify(response), 400


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
            time.sleep(9)
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

        for ser, shard_list in data['servers'].items():
            for x in shard_list:
                if len(shard_hash) == 0 or x not in shard_hash:
                    shard_hash[x] = CH()
                shard_hash[x].add_server_hash(ser, All_servers[ser])

        shard_ids = [shard_info["Shard_id"] for shard_info in data['shards']]
        for sid in shard_ids:
            shard_locks[sid] = ReadWriteLock()
        for ser in servers:
            server_locks[ser] = ReadWriteLock()

        time.sleep(9)
        # shard_ser = db_helper.all_shard_servers()
        # print(shard_ser,flush=True)
        return jsonify({
            'message' : "Configured Database"
        }),200
    except Exception as e:
        print('init error: ',e,flush=True)
        return jsonify({'message':f'Error :{str(e)}'}),500


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
                time.sleep(9)
                response = requests.post(f"http://{ser}:5000/config?id={ser}",json=info)
                if response.status_code == 200:
                    print("Request to", ser, "was successful")
                else:
                    print("Request to", ser, "failed with status code:", response.status_code)
            
            message = f'Add {", ".join(new_servers.keys())}'
            for ser, shard_list in new_servers.items():
                for x in shard_list:
                    if len(shard_hash) == 0 or x not in shard_hash:
                        shard_hash[x] = CH()
                    shard_hash[x].add_server_hash(ser, All_servers[ser])

            newshard_ids = [shard_info["Shard_id"] for shard_info in new_shards]
            for sid in newshard_ids:
                shard_locks[sid] = ReadWriteLock()
            for ser in new_servers.keys():
                server_locks[ser] = ReadWriteLock()

            add_oldshards(newshard_ids, new_servers)
            
            return jsonify({
                'N' : len(All_servers),
                'message': message,
                'status':'successful'
            }),200
            
        else:
            return jsonify({'message':'<Error> Number of new servers (n) is greater than newly added instances',
            'status':'failure'}),400
        

    except Exception as e:
        print('add error: ',e,flush=True)
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
            print('After removing : ',All_servers,flush=True)
            return jsonify({
                'message': {
                    "N": len(All_servers),  
                    "servers": deleted_servers
                        }
                    }), 200

    except Exception as e:
        return jsonify({'message': f'rmError: {str(e)}'}), 500


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
        read_queue = Queue()
        # db_mutex.acquire()
        # try:
        shards_req = db_helper.shards_required(low,high)
        # finally:
        #     db_mutex.release()
        threads = {}
        # print('inside read',shard_locks,flush=True)
        cli_id = request.args.get('id')
        for shard_id in shards_req:
            # print('before',shard_hash,shard_id,flush=True)
            server_name = get_avail_serv(cli_id, shard_hash[shard_id])
            # print('after',shard_hash,shard_id,server_name,flush=True)
            threads[shard_id] = threading.Thread(target=read_to_shard, args=(shard_id, server_name, low, high,read_queue))
            threads[shard_id].start()
    
        for thread in threads.values():
            thread.join()
        
        data_list = []
        while not read_queue.empty():
            result = read_queue.get()
            data_list.append(result)
        all_rows = []

        for item in data_list:
            if 'message' in item and 'data' in item['message']:
                all_rows.extend(item['message']['data'])

        response ={
            "shards_queried":shards_req,
            "data" : all_rows,
            "status" : "success"
        }
        #print(all_rows,flush=True)
        
        return jsonify(response),200
    except Exception as e:
        print("error in read endpoint: ",e,flush=True)
        return jsonify({'message':f'Error :{str(e)}'}),500

@app.route('/write',methods = ['POST'])
def write():
    try:
        data = request.json
        write_shard = {}
        all_rows = data.get('data')
        n = len(all_rows)
        for entry in all_rows:
            print(entry,entry['Stud_id'],flush=True)
            db_mutex.acquire()
            try:
                res = db_helper.studid_to_shard(entry['Stud_id'])
            finally:
                db_mutex.release()
            # print('from db : ',res, type(res),flush=True)
            if len(write_shard) == 0 or res not in write_shard:
                write_shard[res]=[]
            write_shard[res].append(entry)
        # print(write_shard,flush =True)
        threads = {}
        # print('inside write',shard_locks,flush=True)
        for shard_id, entries in write_shard.items():
            threads[shard_id] = threading.Thread(target=write_to_shard, args=(shard_id, entries))
            threads[shard_id].start()
    
        for thread in threads.values():
            thread.join() 

        # print(db_helper.get_all(),flush=True)   
        response = {
            "message" : f"{n} Data entries added",
            "status" : "success"
        }      
        return jsonify(response),200  
    except Exception as e:
        print('error in write end point ',e)
        return jsonify({'message':f'Error :{str(e)}'}),500

@app.route('/update',methods = ['PUT'])
def update():
    try:
        data = request.json
        St_id = data.get('Stud_id')
        row = data.get('data')
        shard_id = db_helper.studid_to_shard(St_id)
        print(shard_id,St_id,row,flush=True)
        info = {
            "shard" : shard_id,
            "Stud_id" : St_id,
            "data" : row
        }
        servers = db_helper.get_shard_servers(shard_id)
        print(servers,flush=True)
        for ser in servers:
            response = requests.put(f"http://{ser}:5000/update?id={ser}",json=info)
            x = json.loads(response.text)
            print(x,flush=True)
        response = {
            "message" : f"Data entry for Stud_id: {St_id} updated",
            "status" : "success"
        } 
        return jsonify(response),200
    except Exception as e:
        print('error while updating ',e,flush=True)
        return jsonify({'message':f'Error :{str(e)}'}),500

@app.route('/delete',methods = ['DELETE'])
def delete():
    try:
        data = request.json
        St_id = data.get('Stud_id')
        shard_id = db_helper.studid_to_shard(St_id)
        info = {
            "shard" : shard_id,
            "Stud_id" : St_id
        }
        print(shard_id,St_id,flush=True)
        servers = db_helper.get_shard_servers(shard_id)
        print(servers,flush=True)
        for ser in servers:
            response = requests.delete(f"http://{ser}:5000/delete?id={ser}",json=info)
            x = json.loads(response.text)
            print(x,flush=True)
        response = {
            "message" : f"Data entry with Stud_id: {St_id} removed",
            "status" : "success"
        } 
        return jsonify(response),200
    except Exception as e:
        print('error while deleting: ',e,flush=True)
        return jsonify({'message':f'Error :{str(e)}'}),500

if __name__ == '__main__':
    app.run(debug = True,host = '0.0.0.0',port = 5000)

