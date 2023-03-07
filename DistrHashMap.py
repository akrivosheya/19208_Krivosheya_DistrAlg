import threading
import time

from connections import Connections

CHECKING_WAIT_TIME = 0.5

#private
#len
class DistrHashMap:
    def __init__(self, ownHost, otherHosts):#проверки
        self.connections = Connections(ownHost, otherHosts, self.__localSet, self.destroy)
        self.hashMap = dict()
        self.checkingCycle = threading.Thread(target=self.__checkMessages)
        self.isDestroying = False
        self.checkingCycle.start()

    def destroy(self):
        self.isDestroying = True
        self.checkingCycle.join()

    def get(self, key):
        return self.hashMap.get(hash(key))

    def set(self, value):
        self.__localSet(value)
        self.connections.setRPC(value)

    def __localSet(self, value):
        self.hashMap[hash(value)] = value

    def __checkMessages(self):
        while True:
            time.sleep(CHECKING_WAIT_TIME)
            if(self.isDestroying):
                break
            self.connections.sendAliveMessage()
            self.connections.checkMessages()
        


