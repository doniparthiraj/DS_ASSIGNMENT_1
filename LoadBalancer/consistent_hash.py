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
        ind = self.extract_id(serid)
        self.serv_no_to_name [ind] = serid
        for j in range(1, K + 1):
            self.serhash(ind, j)

    def check_next(self, ind):
        for i in range(ind,M):
            if len(self.slot[i]) == 0:
                return i
        for i in range(0,ind):
            if len(self.slot[i]) == 0:
                return i
        return -1

    def serhash(self, serid, vid):

        v = serid**2 + vid**2 + 2*vid + 25
        ind = v % M

        ind = self.check_next( ind )
        if(ind == -1):
            return "No slots are empty"
        self.slot[ind].append(serid)
        self.slot[ind].append(vid)
        
    def recv_server_id(self, ind):

        for s in range(ind + 1, M):
            if len(self.slot[s]) != 0:
                return self.slot[s][0]
   
        for s in range(0, ind):
            if len(self.slot[s]) != 0:
                return self.slot[s][0]

        return "No servers added"

    def reqhash(self, reqid):
        v = reqid**2 + 2*reqid + 17
        ind = v % M
        res = self.recv_server_id(ind)
        if res == "No servers added":
            return None
        return self.serv_no_to_name[res]

    def on_ser_failure(self, serid):
        for s in range(0, M):
            if(self.slot[s][0] == serid):

                self.slot[s] = []