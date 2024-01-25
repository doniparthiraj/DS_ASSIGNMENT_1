import re
M = 512
K = 9

class ConsistentHash:

    def __init__(self):
        self.serv_no_to_name = {}
        self.slot = []
        for i in range(0,M):
            self.slot.append([])

    def extract_id(self, serid):
        match = re.search(r'(\d+)$', serid)
        if match:
            ser_id = match.group(1)
            return int(ser_id)

    def add_server_hash(self, serid):
        id = self.extract_id(serid)
        self.serv_no_to_name [id] = serid
        for j in range(1, K + 1):
            self.serhash(id, j)

    def check_next(self, i):
        if(len(self.slot[i]) != 0):
            curr_i = i
            while(len(self.slot[i]) != 0 and i < M):
                i+=1
            if( i == M):
                i = 0
                while(len(self.slot[i]) != 0 and i < curr_i):
                    i+=1
                if(i == curr_i):
                    return -1
        return i

    def serhash(self, serid, vid):

        v = serid**2 + vid**2 + 2*vid + 25
        i = v % M

        i = self.check_next( i )
        if(i == -1):
            return "No slots are empty"
        self.slot[i].append(serid)
        self.slot[i].append(vid)
        
    def recv_server_id(self, i):

        found = False
        for s in range(i + 1, M):
            if len(self.slot[s]) == 0:
                continue
            else:
                found = True
                return self.slot[s][0]
        
        if found == False:
            for s in range(0, i):
                if len(self.slot[s]) == 0:
                    continue
                else:
                    found = True
                    return self.slot[s][0]

        return "No servers added"

    def reqhash(self, reqid):
        v = reqid**2 + 2*reqid + 17
        i = v % M
        res = self.recv_server_id(i)
        if res == "No servers added":
            return None
        return self.serv_no_to_name[res]

    def on_ser_failure(self, serid):
        for s in range(0, M):
            if len(self.slot[s]) == 0 :
                continue
            else:
                if(self.slot[s][1] == serid):
                    self.slot[s] = []
