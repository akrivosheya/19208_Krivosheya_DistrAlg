from raft import RaftNode

class DistrHashMap:
    def __init__(self, ownHost, otherHosts):#проверки
        self.__node = RaftNode(ownHost, otherHosts, self.__localSet, self.__localPop, self.destroy)
        self.__hashMap = dict()

    def destroy(self):
        self.__node.destroy()

    def get(self, key):
        return self.__hashMap.get(hash(key))

    def set(self, key, value):
        self.__node.setRPC(hash(key), value)

    def pop(self, key):
        self.__node.popRPC(hash(key))

    def __localSet(self, key, value):
        self.__hashMap[hash(key)] = value

    def __localPop(self, key):
        self.__hashMap.pop(hash(key))

import time

OWNER_KEY = 'owner'
SLEEP_TIME = 0.1

class HostData:
    def __init__(self, hasRequest, locksCount):
        self.hasRequest = hasRequest
        self.locksCount = locksCount

class AtomicOperations(DistrHashMap):
    def __init__(self, ownHost, otherHosts):
        DistrHashMap.__init__(self, ownHost, otherHosts)
        self.__ownHost = ownHost
        self.__otherHosts = otherHosts
        self.__localSet('OWNER_KEY', None)
        self.__localSet(self.__ownHost, HostData(False, 0))
        for host in self.__otherHosts:
            self.__localSet(host, HostData(False, 0))

    def compareAndSwap(self, reg, oldValue, newValue):
        self.__lock()
        if(reg.value == oldValue):
            reg.value = newValue
            self.__release()
            return True
        else:
            self.__release()
            return False
        
    def __lock(self):
        self.__setOwnData(True, self.get(self.__ownHost).locksCount)
        otherRequests = self.__getOtherRequests()
        while len(otherRequests) > 0:
            if self.get(OWNER_KEY) != None and self.get(OWNER_KEY) in otherRequests:
                self.__setOwnData(False, self.get(self.__ownHost).locksCount)
                while self.get(OWNER_KEY) != None and self.get(OWNER_KEY) != self.__ownHost:
                    time.sleep(SLEEP_TIME)
                self.__setOwnData(True, self.get(self.__ownHost).locksCount)

    def __release(self):
        nextHost = self.__getHostWithMinLocksCount()
        self.__setValueGuaranteed(OWNER_KEY, nextHost)
        self.__setOwnData(False, self.get(self.__ownHost).locksCount + 1)

    def __getOtherRequests(self):
        otherRequests = set()
        for host in self.__otherHosts:
            if self.get(host).hasRequest:
                otherRequests.add(host)
        return otherRequests
    
    def __getHostWithMinLocksCount(self):
        minHost = self.__otherHosts[0]
        for host in self.__otherHosts:
            if self.get(host).locksCount < self.get(minHost).locksCount:
                minHost = host
        return minHost
    
    def __setOwnData(self, hasRequest, locksCount):
        newData = HostData(hasRequest, locksCount)
        self.__setValueGuaranteed(self.__ownHost, newData)

    def __setValueGuaranteed(self, key, value):
        self.set(key, value)
        while not self.get(key) == value:
            time.sleep(SLEEP_TIME)


        