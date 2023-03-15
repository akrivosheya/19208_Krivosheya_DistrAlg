import threading
import time

from connections import Connections

CHECKING_WAIT_TIME = 0.3
TIMEOUT = 5

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
        self.connections.isDestroying = True
        self.checkingCycle.join()
        self.connections.join()

    def get(self, key):
        return self.hashMap.get(key)

    def set(self, key, value):
        #self.__localSet(value)
        self.connections.setRPC(key, value)

    def __localSet(self, key, value):
        self.hashMap[key] = value

    def __checkMessages(self):
        iterations = 0
        while True:
            time.sleep(CHECKING_WAIT_TIME)
            if(self.isDestroying):
                break
            self.connections.sendAliveMessage()
            #self.connections.checkMessages()
            iterations += 1
            if(iterations == TIMEOUT):
                self.connections.checkAliveHosts()
                iterations = 0
        


