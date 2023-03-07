import socket
import select
import threading
import time
import pickle

CHECKING_WAIT_TIME = 0.5
BUF_SIZE = 1024
MESSAGE_TYPE_SIZE = 1
MESSAGE_LENGTH_SIZE = 1

ALIVE = 1
SET = 2

class DistrHashMap:
    def __init__(self, host, port, otherHost, otherPort):#проверки
        self.host = host
        self.port = port
        self.otherHost = otherHost
        self.otherPort = otherPort
        self.descriptors = set()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)#exceptions!!!
        if (port < otherPort):
            self.isLeader = True
            self.socket.bind((host, port))
            self.socket.listen(1)
            self.connection, self.address = self.socket.accept()
            self.descriptors.add(self.connection.fileno())
            self.messagePipe = self.connection
        else:
            self.isLeader = False
            self.socket.connect((otherHost, otherPort))
            self.descriptors.add(self.socket.fileno())
            self.messagePipe = self.socket
        self.hashMap = dict()
        self.checkingCycle = threading.Thread(target=self.check_messages)
        self.isDestroying = False
        self.checkingCycle.start()

    def destroy(self):
        self.isDestroying = True
        self.checkingCycle.join()

    def sendMessage(self, mes):
        self.messagePipe.sendall(mes)#exceptions!!!

    def recieveMessage(self):
        data = self.messagePipe.recv(MESSAGE_TYPE_SIZE)
        #while data.__len__() > 0:
        if data.__len__() > 0:
            messageType = int.from_bytes(data, byteorder='little', signed=False)
            if(messageType & ALIVE):
                pass#что-то для живого узла
            elif(messageType & SET):
                print('type: ', data)
                data = self.messagePipe.recv(MESSAGE_LENGTH_SIZE)
                length = int.from_bytes(data, byteorder='little', signed=False)
                print('length: ', length)
                data = self.messagePipe.recv(length)
                print("serialized: ", data)
                value = pickle.loads(data)
                self.__localSet(value)
            else:
                print('strange data')
                self.destroy

    def get(self, key):
        return self.hashMap.get(hash(key))

    def set(self, value):
        self.__localSet(value)
        serializedValue = pickle.dumps(value)
        print("serialized: ", serializedValue)
        length = serializedValue.__len__().to_bytes(1, byteorder='little', signed=False)
        print("length: ", length)
        messageType = SET.to_bytes(1, byteorder='little', signed=False)
        print("messageType: ", messageType)
        print('message: ', messageType.join([length, serializedValue]))
        self.sendMessage((b'').join([messageType, length, serializedValue]))

    def __localSet(self, value):
        self.hashMap[hash(value)] = value

    def select(self):
        rlist, wlist, xlist = select.select(list(self.descriptors), list(self.descriptors), list(self.descriptors), 1)
        allDescrs = set(rlist + wlist + xlist)
        rlist = set(rlist)
        wlist = set(wlist)
        xlist = set(xlist)
        for descriptor in allDescrs:
            canRead = False
            #canWrite = False
            if descriptor in rlist:
                canRead = True
            """
            if descriptor in wlist:
                canWrite = True
            if descr in xlist:
                event |= 4
            """
        if canRead:
            self.recieveMessage()

    def check_messages(self):
        while True:
            time.sleep(CHECKING_WAIT_TIME)
            if(self.isDestroying):
                break
            self.sendMessage(ALIVE.to_bytes(MESSAGE_TYPE_SIZE, byteorder='little', signed=False))
            self.select()
        


