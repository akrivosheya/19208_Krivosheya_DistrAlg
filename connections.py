import socket
import select
import pickle
import threading
import time

PORT_SEPARATOR = ':'
MESSAGE_TYPE_SIZE = 1
MESSAGE_LENGTH_SIZE = 1
TIMEOUT = 5
ACCEPTING_TIMEOUT = 0.1
CHECKING_WAIT_TIME = 0.3
MAX_ATTEMPTS = 5

ALIVE = 0
SET = 1
GET_LOG = 2
SEND_LOG = 3

#private
#len
class Connections:#получение сообщений и accept лидера в отдельном потоке
    def __init__(self, __ownHost, otherHosts, onRemoteSet, onDestroy):#проверки
        self.__onRemoteSet = onRemoteSet
        self.__onDestroy = onDestroy
        self.__isDestroying = False
        self.__commands = list()
        self.__commands.append(self.__aliveCommand)
        self.__commands.append(self.__setCommand)
        self.__commands.append(self.__getLogCommand)

        separatedHost = __ownHost.split(PORT_SEPARATOR)
        self.__ownHost = (separatedHost[0], int(separatedHost[1]))

        self.__aliveInIterationHosts = dict()
        self.__aliveInIterationHosts[self.__ownHost] = True
        for otherHost in otherHosts:
            separatedHost = otherHost.split(PORT_SEPARATOR)
            self.__aliveInIterationHosts[(separatedHost[0], int(separatedHost[1]))] = False
        self.__aliveHosts = dict()
        self.__aliveHosts[self.__ownHost] = True
        for otherHost in otherHosts:
            separatedHost = otherHost.split(PORT_SEPARATOR)
            self.__aliveHosts[(separatedHost[0], int(separatedHost[1]))] = True

        self.__allHosts = set(self.__aliveInIterationHosts.keys())
        self.__allHosts.add(self.__ownHost)
        self.__otherHostsCount = len(otherHosts)

        self.__connections = dict()
        self.__logs = list()
        self.__setConnection()

        self.__checkingLock = threading.Lock()
        self.__checkingMessagesThread = threading.Thread(target=self.__checkMessages)
        self.__checkingMessagesThread.start()
        self.__checkingHostsThread = threading.Thread(target=self.__checkHosts)
        self.__checkingHostsThread.start()

    def stop(self):
        self.__isDestroying = True
        self.__checkingMessagesThread.join()
        self.__checkingHostsThread.join()

    def setRPC(self, key, value):
        message = self.__sendMessage(SET, (key, value))
        if self.__isLeader:
            self.__onRemoteSet(key, value)
            self.__logs.append(message)

    def __sendAliveMessage(self, host):
        self.__sendMessage(ALIVE, host)

    def __getLogMessage(self):
        self.__sendMessage(GET_LOG, len(self.__logs))

    def __sendLogMessage(self, log, pipe):
        """
        serializedValue = log
        length = serializedValue.__len__().to_bytes(MESSAGE_LENGTH_SIZE, byteorder='little', signed=False)
        messageType = SEND_LOG.to_bytes(MESSAGE_TYPE_SIZE, byteorder='little', signed=False)
        pipe.sendall((b'').join([messageType, length, serializedValue]))
        """
        pipe.sendall(log)

    def __sendMessage(self, messageType, value):
        serializedValue = pickle.dumps(value)
        length = serializedValue.__len__().to_bytes(MESSAGE_LENGTH_SIZE, byteorder='little', signed=False)
        serializedMessageType = messageType.to_bytes(MESSAGE_TYPE_SIZE, byteorder='little', signed=False)
        message = (b'').join([serializedMessageType, length, serializedValue])
        self.__sendReadyMessage(message)
        return message

    def __sendReadyMessage(self, mes):
        if self.__isLeader:
            for address, connection in self.__connections.items():
                try:
                    connection.sendall(mes)
                except:
                    self.__aliveInIterationHosts[address] = False
        else:
            try:
                self.__socket.sendall(mes)#exceptions!!!
            except:
                self.__aliveInIterationHosts[self.__socket.getpeername()] = False

    def __checkHosts(self):
        iterations = 0
        while True:
            time.sleep(CHECKING_WAIT_TIME)
            if(self.__isDestroying):
                break
            self.__sendAliveMessage(self.__ownHost)
            iterations += 1
            if(iterations == TIMEOUT):
                self.__checkAliveHosts()
                iterations = 0

    def __checkAliveHosts(self):
        self.__checkingLock.acquire()
        needNewLeader = False
        for address, isAlive in self.__aliveInIterationHosts.items():
            if isAlive:
                self.__aliveHosts[address] = True
                if address != self.__ownHost:
                    self.__aliveInIterationHosts[address] = False
            else:
                self.__aliveHosts[address] = False
                if self.__connections.get(address) != None:
                    self.__connections.pop(address).close()
                if self.__ownHost != self.__leader and address == self.__leader:
                    needNewLeader = True
        if needNewLeader:
            self.__setConnection()
        self.__checkingLock.release()

    def __checkMessages(self):
        while True:
            if self.__isDestroying:
                addresses = list(self.__connections.keys())
                for address in addresses:
                    self.__connections.pop(address).close()
                self.__socket.close()
                return
            checkingPipes = list()
            if self.__isLeader:
                self.__socket.settimeout(ACCEPTING_TIMEOUT)
                try:
                    connection, address = self.__socket.accept()
                    print('accepted ', address)
                    self.__checkingLock.acquire()
                    self.__connections[address] = connection
                except TimeoutError:
                    self.__checkingLock.acquire()
                    pass
                for connection in self.__connections.values():
                    checkingPipes.append(connection)
            else:
                self.__checkingLock.acquire()
                checkingPipes.append(self.__socket)
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
            self.__checkingLock.release()

    def __recieveMessage(self, pipe):
        try:
            pipe.setblocking(False)
            serializedMessageType = pipe.recv(MESSAGE_TYPE_SIZE)
            if serializedMessageType.__len__() > 0:
                messageType = int.from_bytes(serializedMessageType, byteorder='little', signed=False)
                serializedLength = pipe.recv(MESSAGE_LENGTH_SIZE)
                length = int.from_bytes(serializedLength, byteorder='little', signed=False)
                serializedData = pipe.recv(length)
                self.__commands[messageType](serializedMessageType, serializedLength, serializedData, pipe)
        except Exception as ex:
            print('Lost connection to host: ', ex)
            try:
                self.__aliveInIterationHosts[pipe.getpeername()] = False
            except:
                print("can't get info about host")
            return
    
    def __aliveCommand(self, serializedMessageType, serializedLength, serializedData, pipe):
        host = pickle.loads(serializedData)
        self.__aliveInIterationHosts[host] = True
        if(self.__isLeader):
            self.__sendAliveMessage(host)

    def __setCommand(self, serializedMessageType, serializedLength, serializedData, pipe):
        key, value = pickle.loads(serializedData)
        print("set")
        self.__onRemoteSet(key, value)
        self.__logs.append((b'').join([serializedMessageType, serializedLength, serializedData]))
        self.__aliveInIterationHosts[pipe.getpeername()] = True
        if(self.__isLeader):
            self.__sendReadyMessage((b'').join([serializedMessageType, serializedLength, serializedData]))

    def __getLogCommand(self, messageType, serializedLength, serializedData, pipe):
        lastLog = pickle.loads(serializedData)
        print("get")
        for i in range(lastLog, len(self.__logs)):
            self.__sendLogMessage(self.__logs[i], pipe)

    def __setConnection(self):#возможно нужно усложнить получение информации о лидерах и добавление старого лидера
        addresses = list(self.__connections.keys())
        for address in addresses:
            self.__connections.pop(address).close()
        try:
            self.__socket.close()
        except:
            pass
        #до этого нужно убрать
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)#exceptions!!!
        self.__socket.bind(self.__ownHost)
        self.__socket.settimeout(TIMEOUT)
        self.__leader = self.__getLeader(list(self.__allHosts))
        if (self.__ownHost == self.__leader):
            self.__isLeader = True
            self.__socket.listen(1)
            minNodesCount = self.__otherHostsCount / 2
            while(self.__connections.__len__() < minNodesCount):
                try:
                    connection, address = self.__socket.accept()
                    print('accepted ', address)
                    self.__connections[address] = connection
                except:
                    print('bad accept')
                    self.__onDestroy()
        else:
            self.__isLeader = False
            connected = False
            for i in range(MAX_ATTEMPTS):
                print('attempt ', i, " to connect to ", self.__leader)
                try:
                    self.__socket.connect(self.__leader)
                    connected = True
                    break
                except:
                    print("failed attempt ", i, " to connect to ", self.__leader)
            if connected:
                print('connected to ', self.__leader)
                self.__getLogMessage()
            else:
                print("Can't connect to leader")
                self.__onDestroy()

    def __getLeader(self, hosts):#проверки + одинаковые порты
        leader = hosts[0]
        for host in hosts[1:len(hosts)]:
            if (not self.__aliveHosts[leader]) or (self.__aliveHosts[host] and host[1] < leader[1]):
                leader = host
        print(leader)
        return leader

        