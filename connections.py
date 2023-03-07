import socket
import select
import pickle

PORT_SEPARATOR = ':'
MESSAGE_TYPE_SIZE = 1
MESSAGE_LENGTH_SIZE = 1
TIMEOUT = 5
MAX_ATTEMPTS = 5

ALIVE = 1
SET = 2

#private
#len
class Connections:
    def __init__(self, ownHost, otherHosts, onRemoteSet, onDestroy):#проверки
        self.onRemoteSet = onRemoteSet
        self.onDestroy = onDestroy
        separatedHost = ownHost.split(PORT_SEPARATOR)
        self.ownHost = (separatedHost[0], int(separatedHost[1]))
        self.aliveHosts = dict()
        for otherHost in otherHosts:
            otherHostSeparated = otherHost.split(PORT_SEPARATOR)
            self.aliveHosts[(otherHostSeparated[0], int(otherHostSeparated[1]))] = False
        self.descriptors = set()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)#exceptions!!!
        allHosts = list(self.aliveHosts.keys())
        allHosts.append(self.ownHost)
        self.leader = self.__getLeader(allHosts)
        if (self.ownHost == self.leader):#м\б сложная проверка
            self.isLeader = True
            self.socket.bind(self.ownHost)
            self.socket.listen(1)

            minNodesCount = len(otherHosts) / 2
            self.connections = dict()
            while(self.connections.__len__() < minNodesCount):
                self.connection, self.address = self.socket.accept()
                self.connections[self.address] = self.connection
                self.descriptors.add(self.connection.fileno())
                self.messagePipe = self.connection
        else:
            self.isLeader = False
            self.socket.settimeout(TIMEOUT)
            connected = False
            for i in range(MAX_ATTEMPTS):
                print('attempt ', i)
                try:
                    self.socket.connect(self.leader)
                    self.descriptors.add(self.socket.fileno())
                    self.messagePipe = self.socket
                    connected = True
                    break
                except TimeoutError:
                    print("failed attempt ", i)
            if connected:
                print('connected')
            else:
                raise TimeoutError("Can't connect to leader")

    def setRPC(self, value):
        serializedValue = pickle.dumps(value)
        print("serialized: ", serializedValue)
        length = serializedValue.__len__().to_bytes(1, byteorder='little', signed=False)
        print("length: ", length)
        messageType = SET.to_bytes(1, byteorder='little', signed=False)
        print("messageType: ", messageType)
        print('message: ', messageType.join([length, serializedValue]))
        self.__sendMessage((b'').join([messageType, length, serializedValue]))

    def checkMessages(self):
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
            self.__recieveMessage()

    def sendAliveMessage(self):
        self.__sendMessage(ALIVE.to_bytes(MESSAGE_TYPE_SIZE, byteorder='little', signed=False))

    def __recieveMessage(self):
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
                self.onRemoteSet(value)
            else:
                print('strange data')
                self.onDestroy()

    def __sendMessage(self, mes):
        self.messagePipe.sendall(mes)#exceptions!!!

    def __getLeader(self, hosts):#проверки + одинаковые порты
        leader = hosts[0]
        for host in hosts[1:len(hosts)]:
            if host[1] < leader[1]:
                leader = host
        return leader

        