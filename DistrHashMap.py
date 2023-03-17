from connections import Connections

class DistrHashMap:
    def __init__(self, ownHost, otherHosts):#проверки
        self.__connections = Connections(ownHost, otherHosts, self.__localSet, self.destroy)
        self.__hashMap = dict()

    def destroy(self):
        self.__connections.stop()

    def get(self, key):
        return self.__hashMap.get(key)

    def set(self, key, value):
        self.__connections.setRPC(key, value)

    def __localSet(self, key, value):
        self.__hashMap[key] = value
        


