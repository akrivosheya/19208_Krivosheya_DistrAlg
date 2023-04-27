from raft import RaftNode

DIV = 100

class DistrHashMap:
    def __init__(self, ownHost, otherHosts):#проверки
        self.__node = RaftNode(ownHost, otherHosts, self.localSet, self.localPop, self.destroy)
        self.__hashMap = dict()

    def destroy(self):
        self.__node.destroy()

    def get(self, key):
        return self.__hashMap.get(self.__hash(key))

    def set(self, key, value):
        self.__node.setRPC(key, value)

    def pop(self, key):
        self.__node.popRPC(key)

    def localSet(self, key, value):
        self.__hashMap[self.__hash(key)] = value

    def localPop(self, key):
        self.__hashMap.pop(self.__hash(key))

    def __hash(self, key):
        h = 0
        lim = len(key)
        for i in range(0, lim):
            h = (h * 31 + int.from_bytes(key[i].encode(), byteorder='little', signed=False)) % DIV
        return h

import time

OWNER_KEY = 'owner'
REQUEST_EXKEY = 'hasRequest'
LOCKS_EXKEY = 'locksCount'

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
        self.localSet(OWNER_KEY, self.__getMinHost())
        self.localSet(REQUEST_EXKEY + self.__ownHost, False)
        self.localSet(LOCKS_EXKEY + self.__ownHost, 0)
        for host in self.__otherHosts:
            self.localSet(REQUEST_EXKEY + host, False)
            self.localSet(LOCKS_EXKEY + host, 0)

    def compareAndSwap(self, reg, oldValue, newValue):
        print('try lock')
        self.__lock()
        print('locked')
        if(reg.value == oldValue):
            reg.value = newValue
            print('try release')
            self.__release()
            print('released')
            return True
        else:
            print('try release')
            self.__release()
            print('released')
            return False
        
    def __lock(self):
        #print('try set own data')
        self.__setOwnData(True, self.get(LOCKS_EXKEY + self.__ownHost))
        #print('set own data')
        otherRequests = self.__getOtherRequests()
        '''print('requests:')
        for request in otherRequests:
            print('    request', request)
        print('owner', self.get(OWNER_KEY))'''
        while len(otherRequests) > 0:
            if self.get(OWNER_KEY) != None and self.get(OWNER_KEY) in otherRequests:
                #print('try set own data False')
                self.__setOwnData(False, self.get(LOCKS_EXKEY + self.__ownHost))
                #print('set own data False')
                while self.get(OWNER_KEY) != None and self.get(OWNER_KEY) in otherRequests:
                    time.sleep(SLEEP_TIME)
                #print('try set own data True')
                self.__setOwnData(True, self.get(LOCKS_EXKEY + self.__ownHost))
                #print('set own data True')
            otherRequests = self.__getOtherRequests()
            '''print('requests in cycle:')
            for request in otherRequests:
                print('    request', request)'''

    def __release(self):
        nextHost = self.__getHostWithMinLocksCount()
        self.__setOwnerGuaranteed(nextHost)
        self.__setOwnData(False, self.get(REQUEST_EXKEY + self.__ownHost) + 1)

    def __getOtherRequests(self):
        otherRequests = set()
        for host in self.__otherHosts:
            if self.get(REQUEST_EXKEY + host):
                otherRequests.add(host)
        return otherRequests
    
    def __getHostWithMinLocksCount(self):
        minHost = self.__otherHosts[0]
        for host in self.__otherHosts:
            if self.get(LOCKS_EXKEY + host) < self.get(LOCKS_EXKEY + minHost):
                minHost = host
        return minHost
    
    def __getMinHost(self):
        hosts = list(self.__otherHosts)
        hosts.append(self.__ownHost)
        minHost = hosts[0]
        for host in hosts:
            if host < minHost:
                minHost = host
        return minHost
    
    def __setOwnData(self, hasRequest, locksCount):
        self.__setValueGuaranteed(self.__ownHost, hasRequest, locksCount)

    def __setValueGuaranteed(self, host, hasRequest, locksCount):
        self.set(REQUEST_EXKEY + host, hasRequest)
        self.set(LOCKS_EXKEY + host, locksCount)
        while not (self.get(REQUEST_EXKEY + host) == hasRequest and self.get(LOCKS_EXKEY + host) == locksCount):
            time.sleep(SLEEP_TIME)

    def __setOwnerGuaranteed(self, host):
        self.set(OWNER_KEY, host)
        while not (self.get(OWNER_KEY) == host):
            time.sleep(SLEEP_TIME)


        