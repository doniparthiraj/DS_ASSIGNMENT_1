import re
slot = []
M = 512
K = 9
mappings = {}
arrange = {}
reqsList = []

def on_ser_failure(serid):
    for s in range(0,M):
        if len(slot[s]) == 0 :
            continue
        else:
            if(slot[s][1] == serid):
                slot[s] = []

def check_next(i):
    if(len(slot[i]) != 0):
        curr_i = i
        while(len(slot[i]) != 0 and i < M):
            i+=1
        if( i == M):
            i = 0
            while(len(slot[i]) != 0 and i < curr_i):
                i+=1
            if(i == curr_i):
                return -1

    return i

def serhash(serid,vid):
    v = (serid*serid) + (vid*vid) + (2*vid) + 25
    i = v%M

    i = check_next(i)
    if(i == -1):
        return "No slots are empty"
    slot[i].append(serid)
    slot[i].append(vid)
    

def recv_server_id(i):

    found = False
    server = None
    for s in range(i+1,M):
        if len(slot[s]) == 0:
            continue
        else:
            found = True
            server = tuple(slot[s])
            return slot[s][0]
    
    if found == False:
        for s in range(0,i):
            if len(slot[s]) == 0:
                continue
            else:
                found = True
                server = tuple(slot[s])
                return slot[s][0]

    return "No servers added"

def reqhash(reqid):
	v = (reqid)*(reqid) + 2*(reqid) + 17
	i = v%M
	res = recv_server_id(i)
	if res == "No servers added":
		return -1
	return res


def extract_id(serid):
	match = re.search(r'(\d+)$', serid)
	if match:
		ser_id = match.group(1)
		return ser_id
	else:
		return -1


def add_server_hash(serid):
	for j in range(1,K+1):
		serhash(serid,j)

def initiate_slot():
	for i in range(0,M):
		slot.append([])



