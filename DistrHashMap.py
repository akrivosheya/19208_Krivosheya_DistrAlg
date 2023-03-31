from raft import RaftNode

class DistrHashMap:
    def __init__(self, ownHost, otherHosts):#проверки
        self.__node = RaftNode(ownHost, otherHosts, self.__localSet, self.__localPop, self.destroy)
        self.__hashMap = dict()

    def destroy(self):
        self.__node.destroy()

    def get(self, key):
        return self.__hashMap.get(key)

    def set(self, key, value):
        self.__node.setRPC(key, value)

    def pop(self, key):
        self.__node.popRPC(key)

    def __localSet(self, key, value):
        self.__hashMap[key] = value

    def __localPop(self, key):
        self.__hashMap.pop(key)
        