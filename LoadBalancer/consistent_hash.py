import random
M = 512
K = 9

class ConsistentHash:

    def __init__(self):
        self.serv_no_to_name = {}
        self.slot = []
        for i in range(0,M):
            self.slot.append([])

    def check_next(self, ind):
        #linear probing
        for i in range(ind,M):
            if len(self.slot[i]) == 0:
                return i
        for i in range(0,ind):
            if len(self.slot[i]) == 0:
                return i
        return -1

    def serhash(self, serid, vid):
        #hashing the id to place the server
        v = serid**2 + vid**2 + 2*vid + 25
        ind = v % M
        ind = self.check_next( ind )
        if(ind == -1):
            return "No slots are empty"     #need to extend
        self.slot[ind].append(serid)
        self.slot[ind].append(vid)

    def add_server_hash(self, sername, serid):
        #adding all virtual servers
        self.serv_no_to_name [serid] = sername
        for j in range(1, K + 1):
            self.serhash(serid, j)
        
    def recv_server_id(self, ind):
        #sending near serverid to client
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

    def rem_server(self, serid):
        for s in range(0, M):
            if(len(self.slot[s]) != 0 and self.slot[s][0] == serid):
                self.slot[s] = []

    
    def mod_reqhash(self,reqid):
        v = reqid**2 + 2*reqid + 17 + random.randint(0,10000)
        ind = v % M
        res = self.recv_server_id(ind)
        if res == "No servers added":
            return None
        return self.serv_no_to_name[res]
    
    def mod_serhash(self,serid,vid):
        v = serid**2 + vid**2 + 2*vid + 25 + random.randint(0,10000)
        ind = v % M
        ind = self.check_next( ind )
        if(ind == -1):
            return "No slots are empty"     #need to extend
        self.slot[ind].append(serid)
        self.slot[ind].append(vid)

