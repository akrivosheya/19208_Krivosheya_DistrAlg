import socket
import select
import pickle

PORT_SEPARATOR = ':'
MESSAGE_TYPE_SIZE = 1
MESSAGE_LENGTH_SIZE = 1
TIMEOUT = 5
ACCEPTING_TIMEOUT = 0.1
MAX_ATTEMPTS = 5

ALIVE = 1
SET = 2
GET_LOG = 4
SEND_LOG = 8

#private
#len
class Connections:#получение сообщений и accept лидера в отдельном потоке
    def __init__(self, ownHost, otherHosts, onRemoteSet, onDestroy):#проверки
        self.onRemoteSet = onRemoteSet
        self.onDestroy = onDestroy
        self.isDestroying = False
        separatedHost = ownHost.split(PORT_SEPARATOR)
        self.ownHost = (separatedHost[0], int(separatedHost[1]))
        self.aliveInIterationHosts = dict()
        self.aliveInIterationHosts[self.ownHost] = True
        for otherHost in otherHosts:
            otherHostSeparated = otherHost.split(PORT_SEPARATOR)
            self.aliveInIterationHosts[(otherHostSeparated[0], int(otherHostSeparated[1]))] = False
        self.aliveHosts = dict()
        self.aliveHosts[self.ownHost] = True
        for otherHost in otherHosts:
            otherHostSeparated = otherHost.split(PORT_SEPARATOR)
            self.aliveHosts[(otherHostSeparated[0], int(otherHostSeparated[1]))] = True
        #self.descriptors = set()
        self.possibleLeaders = set(self.aliveInIterationHosts.keys())
        self.possibleLeaders.add(self.ownHost)
        self.otherHostsCount = len(otherHosts)
        self.connections = dict()
        self.logs = list()
        self.__setConnection()

    def setRPC(self, key, value):
        serializedValue = pickle.dumps((key, value))
        length = serializedValue.__len__().to_bytes(MESSAGE_LENGTH_SIZE, byteorder='little', signed=False)
        messageType = SET.to_bytes(MESSAGE_TYPE_SIZE, byteorder='little', signed=False)
        message = (b'').join([messageType, length, serializedValue])
        self.__sendMessage((b'').join([messageType, length, serializedValue]))
        if self.isLeader:
            self.onRemoteSet(key, value)
            self.logs.append(message)

    def checkMessages(self):
        checkingPipes = list()
        if self.isLeader:
            self.socket.settimeout(ACCEPTING_TIMEOUT)
            try:
                connection, address = self.socket.accept()
                print('accepted ', address)
                self.connections[address] = connection
            except TimeoutError:
                pass
            for connection in self.connections.values():
                checkingPipes.append(connection)
        else:
            checkingPipes.append(self.socket)
        for pipe in checkingPipes:
            descriptor = pipe.fileno()
            rlist, wlist, xlist = select.select([descriptor], [descriptor], [descriptor], 1)
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
                self.__recieveMessage(pipe)

    def sendAliveMessage(self):
        self.__sendAliveMessage(self.ownHost)

    def __sendAliveMessage(self, host):
        serializedValue = pickle.dumps(host)
        length = serializedValue.__len__().to_bytes(MESSAGE_LENGTH_SIZE, byteorder='little', signed=False)
        messageType = ALIVE.to_bytes(MESSAGE_TYPE_SIZE, byteorder='little', signed=False)
        self.__sendMessage((b'').join([messageType, length, serializedValue]))

    def __getLogMessage(self):
        serializedValue = pickle.dumps(len(self.logs))
        length = serializedValue.__len__().to_bytes(MESSAGE_LENGTH_SIZE, byteorder='little', signed=False)
        messageType = GET_LOG.to_bytes(MESSAGE_TYPE_SIZE, byteorder='little', signed=False)
        self.__sendMessage((b'').join([messageType, length, serializedValue]))

    def __sendLogMessage(self, log, pipe):
        """
        serializedValue = log
        length = serializedValue.__len__().to_bytes(MESSAGE_LENGTH_SIZE, byteorder='little', signed=False)
        messageType = SEND_LOG.to_bytes(MESSAGE_TYPE_SIZE, byteorder='little', signed=False)
        pipe.sendall((b'').join([messageType, length, serializedValue]))
        """
        pipe.sendall(log)

    def __recieveMessage(self, pipe):
        try:
            serializedMessageType = pipe.recv(MESSAGE_TYPE_SIZE)
        except:
            print('Lost connection to ', pipe.getpeername())
            self.aliveInIterationHosts[pipe.getpeername()] = False
            return
        #while serializedMessageType.__len__() > 0:
        if serializedMessageType.__len__() > 0:
            messageType = int.from_bytes(serializedMessageType, byteorder='little', signed=False)
            if(messageType & ALIVE):
                serilizedLength = pipe.recv(MESSAGE_LENGTH_SIZE)
                length = int.from_bytes(serilizedLength, byteorder='little', signed=False)
                serializedData = pipe.recv(length)
                host = pickle.loads(serializedData)
                self.aliveInIterationHosts[host] = True
                if(self.isLeader):
                    self.__sendAliveMessage(host)
            elif(messageType & SET):
                serilizedLength = pipe.recv(MESSAGE_LENGTH_SIZE)
                length = int.from_bytes(serilizedLength, byteorder='little', signed=False)
                serializedData = pipe.recv(length)
                key, value = pickle.loads(serializedData)
                print("set")
                self.onRemoteSet(key, value)
                self.logs.append((b'').join([serializedMessageType, serilizedLength, serializedData]))
                self.aliveInIterationHosts[pipe.getpeername()] = True
                if(self.isLeader):
                    self.__sendMessage((b'').join([serializedMessageType, serilizedLength, serializedData]))
            elif(messageType & GET_LOG):
                serilizedLength = pipe.recv(MESSAGE_LENGTH_SIZE)
                length = int.from_bytes(serilizedLength, byteorder='little', signed=False)
                serializedData = pipe.recv(length)
                lastLog = pickle.loads(serializedData)
                print("get")
                for i in range(lastLog, len(self.logs)):
                    self.__sendLogMessage(self.logs[i], pipe)
            else:
                print('strange data')
                self.onDestroy()#исправить
            """
            elif(messageType & SEND_LOG):
                serilizedLength = pipe.recv(MESSAGE_LENGTH_SIZE)
                length = int.from_bytes(serilizedLength, byteorder='little', signed=False)
                lastLog = pipe.recv(length)
                lastLog = pickle.loads(serializedData)
                print("get")
            """
            """
            try:
                serializedMessageType = pipe.recv(MESSAGE_TYPE_SIZE)
            except:
                print('Lost connection to ', pipe.getpeername())
                self.aliveInIterationHosts[pipe.getpeername()] = False
                return
            """

    def __sendMessage(self, mes):
        if self.isLeader:
            for address, connection in self.connections.items():
                try:
                    connection.sendall(mes)
                except:
                    self.aliveInIterationHosts[address] = False
        else:
            try:
                self.socket.sendall(mes)#exceptions!!!
            except:
                self.aliveInIterationHosts[self.socket.getpeername()] = False

    def checkAliveHosts(self):
        needNewLeader = False
        for address, isAlive in self.aliveInIterationHosts.items():
            if isAlive:
                print(address, ' is alive')
                self.aliveHosts[address] = True
                if address != self.ownHost:
                    self.aliveInIterationHosts[address] = False
            else:
                self.aliveHosts[address] = False
                if self.connections.get(address) != None:
                    self.connections.pop(address).close()
                if self.ownHost != self.leader and address == self.leader:
                    needNewLeader = True
        if needNewLeader:
            self.__setConnection()

    def __setConnection(self):#возможно нужно усложнить получение информации о лидерах и добавление старого лидера
        addresses = list(self.connections.keys())
        for address in addresses:
            self.connections.pop(address).close()
        try:
            self.socket.close()
        except:
            pass
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)#exceptions!!!
        self.socket.bind(self.ownHost)
        self.socket.settimeout(TIMEOUT)
        self.leader = self.__getLeader(list(self.possibleLeaders))
        if (self.ownHost == self.leader):
            self.isLeader = True
            self.socket.listen(1)
            minNodesCount = self.otherHostsCount / 2
            while(self.connections.__len__() < minNodesCount):
                connection, address = self.socket.accept()
                print('accepted ', address)
                self.connections[address] = connection
        else:
            self.isLeader = False
            connected = False
            for i in range(MAX_ATTEMPTS):
                print('attempt ', i, " to connect to ", self.leader)
                try:
                    self.socket.connect(self.leader)
                    connected = True
                    break
                except TimeoutError:
                    print("failed attempt ", i, " to connect to ", self.leader)
            if connected:
                print('connected to ', self.leader)
                self.__getLogMessage()
            else:
                raise TimeoutError("Can't connect to leader")


    def __getLeader(self, hosts):#проверки + одинаковые порты
        leader = hosts[0]
        for host in hosts[1:len(hosts)]:
            print(leader, " ", self.aliveHosts[host], " ", host)
            if (not self.aliveHosts[leader]) or (self.aliveHosts[host] and host[1] < leader[1]):
                leader = host
        print(leader)
        return leader

        