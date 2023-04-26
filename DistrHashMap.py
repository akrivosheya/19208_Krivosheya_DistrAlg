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

class AtomicOperations(DistrHashMap):
    def __init__(self, ownHost, otherHosts):
        DistrHashMap.__init__(self, ownHost, otherHosts)
        self.__localSet('lock', True)
        self.__localSet('owner', None)

    def compareAndSwap(self, reg, oldValue, newValue):
        self.__lock()
        if(reg.value == oldValue):
            reg.set(newValue)
            self.__release()
            return True
        else:
            self.__release()
            return False
        
    def __lock(self):
        pass

    def __release(self):
        pass
        