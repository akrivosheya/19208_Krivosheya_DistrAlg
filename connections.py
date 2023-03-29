import socket
import select
import pickle
import threading
import time

PORT_SEPARATOR = ':'

MESSAGE_TYPE_SIZE = 1
MESSAGE_LENGTH_SIZE = 1
MESSAGE_SESSION_SIZE = 1
INDEX_SIZE = 1
BUFFER_SIZE_LIMIT = 100

TIMEOUT = 5
CONNECT_TIME = 1
SELECT_TIME = 1
ACCEPTING_TIMEOUT = 5
CHECKING_WAIT_TIME = 0.5
MAX_ATTEMPTS = 5

ALIVE = 0
SET = 1
GET_LOG = 2
SEND_LOG = 3

class PerfectLink:
    def __init__(self, ownHost, otherHosts, onDestroy):
        self.__ownNode = ownHost
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)#exceptions!!!
        self.__socket.bind(self.__ownNode)
        self.__socket.settimeout(ACCEPTING_TIMEOUT)
        self.__socket.listen(1)
        self.__buffer = list()

        self.__isDestroying = False
        self.__onDestroy = onDestroy
        self.__readConnections = dict()
        self.__writeConnections = dict()
        for otherHost in otherHosts:
            self.__writeConnections[otherHost] = None

        self.__bufferLock = threading.Lock()
        self.__checkingSocketsThread = threading.Thread(target=self.__checkSockets)
        self.__checkingSocketsThread.start()
        self.__acceptingThread = threading.Thread(target=self.__accept)
        self.__acceptingThread.start()

    def getMessages(self):
        self.__bufferLock.acquire()
        tmpBuffer = self.__buffer.copy()
        self.__buffer.clear()
        self.__bufferLock.release()
        return tmpBuffer

    def sendBroadcastMessage(self, messageType, value, session, prevLogIndex, prevLogSession, leaderCommit):
        brokenSockets = list()
        for node in self.__writeConnections.keys():
            if(node == self.__ownNode):
                continue
            if(self.sendMessageTo(messageType, value, node, session, prevLogIndex, prevLogSession, leaderCommit)):
                pass
            else:
                brokenSockets.append(node)
        return brokenSockets

    def sendMessageTo(self, messageType, value, node, session, prevLogIndex, prevLogSession, leaderCommit):
        serializedSession = session.to_bytes(MESSAGE_SESSION_SIZE, byteorder='little', signed=False)
        serializedNode = pickle.dumps(self.__ownNode)
        nodeLength = serializedNode.__len__().to_bytes(MESSAGE_LENGTH_SIZE, byteorder='little', signed=False)
        serializedValue = pickle.dumps(value)
        length = serializedValue.__len__().to_bytes(MESSAGE_LENGTH_SIZE, byteorder='little', signed=False)
        serializedMessageType = messageType.to_bytes(MESSAGE_TYPE_SIZE, byteorder='little', signed=False)
        serializedPrevLog = prevLogIndex.to_bytes(INDEX_SIZE, byteorder='little', signed=False)
        serializedPrevLogSession = prevLogSession.to_bytes(INDEX_SIZE, byteorder='little', signed=False)
        serializedLeaderCommit = leaderCommit.to_bytes(INDEX_SIZE, byteorder='little', signed=False)
        message = (b'').join([serializedMessageType, serializedSession, nodeLength, serializedNode, 
                              serializedPrevLog, serializedPrevLogSession, serializedLeaderCommit, length, serializedValue])
        if(self.__writeConnections[node] == None):
            connection = self.__tryToConnect(node)
            if(connection == None):
                print('no connection ', node)
                return
            else:
                self.__writeConnections[node] = connection
        try:
            self.__writeConnections[node].sendall(message)
        except Exception as ex:
            print("Can't send message: ", ex)
            try:
                self.__writeConnections[node].close()
            except:
                pass
            self.__writeConnections[node] = None
            return False
        return True

    def destroy(self):
        self.__isDestroying = True
        self.__checkingSocketsThread.join()

    def __checkSockets(self):
        while True:
            if self.__isDestroying:
                self.__closeConnections(self.__writeConnections)
                self.__closeConnections(self.__readConnections)
                self.__socket.close()
                return
            checkingSockets = list()
            if(len(self.__buffer) >= BUFFER_SIZE_LIMIT):
                continue
            for connection in self.__readConnections.values():
                if(connection != None):
                    checkingSockets.append(connection)
            for socket in checkingSockets:
                descriptor = socket.fileno()
                rlist, wlist, xlist = select.select([descriptor], [], [], SELECT_TIME)
                rlist = set(rlist)
                if descriptor in rlist:
                    self.__receiveMessage(socket)

    def __accept(self):
        while True:
            if self.__isDestroying:
                return
            try:
                connection, address = self.__socket.accept()
                print('accepted ', address)
                self.__bufferLock.acquire()
                if(address in set(self.__readConnections.keys()) and self.__readConnections[address] != None):
                    self.__readConnections[address].close()
                self.__readConnections[address] = connection
                self.__bufferLock.release()
            except Exception as ex:
                print('no accept ', ex)
                pass

    def __receiveMessage(self, socket):
        try:
            socket.setblocking(False)
            serializedMessageType = socket.recv(MESSAGE_TYPE_SIZE)
            if serializedMessageType.__len__() > 0:
                messageType = int.from_bytes(serializedMessageType, byteorder='little', signed=False)
                serializedSession = socket.recv(MESSAGE_SESSION_SIZE)
                session = int.from_bytes(serializedSession, byteorder='little', signed=False)
                serializedNodeLength = socket.recv(MESSAGE_LENGTH_SIZE)
                nodeLength = int.from_bytes(serializedNodeLength, byteorder='little', signed=False)
                serializedNodeData = socket.recv(nodeLength)
                nodeData = pickle.loads(serializedNodeData)
                serializedPrevLogIndex = socket.recv(INDEX_SIZE)
                prevLogIndex = int.from_bytes(serializedPrevLogIndex, byteorder='little', signed=False)
                serializedPrevLogSession = socket.recv(INDEX_SIZE)
                prevLogSession = int.from_bytes(serializedPrevLogSession, byteorder='little', signed=False)
                serializedLeaderCommit = socket.recv(INDEX_SIZE)
                leaderCommit = int.from_bytes(serializedLeaderCommit, byteorder='little', signed=False)
                serializedLength = socket.recv(MESSAGE_LENGTH_SIZE)
                length = int.from_bytes(serializedLength, byteorder='little', signed=False)
                serializedData = socket.recv(length)
                data = pickle.loads(serializedData)
                self.__bufferLock.acquire()
                self.__buffer.append((messageType, session, nodeData, prevLogIndex, prevLogSession, leaderCommit, data))
                self.__bufferLock.release()
        except Exception as ex:
            print('Lost connection to host: ', ex)
            try:
                print('host: ', socket.getpeername())
            except:
                print("can't get info about host")
        
    def __tryToConnect(self, node):
        connected = False
        newSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        newSocket.settimeout(CONNECT_TIME)
        for i in range(MAX_ATTEMPTS):
            try:
                newSocket.connect(node)
                connected = True
                break
            except Exception as ex:
                #print(ex)
                pass
        if connected:
            print('connected to ', node)
            return newSocket
        else:
            print("Can't connect to ", node)
            newSocket.close()
            return None
        
    def __closeConnections(self, connections):
        hosts = list(connections.keys())
        for host in hosts:
            socket = connections.pop(host)
            if socket != None:
                socket.close()

#private
#len
class Connections:#получение сообщений и accept лидера в отдельном потоке
    def __init__(self, ownHost, otherHosts, onRemoteSet, onDestroy):#проверки
        self.__onRemoteSet = onRemoteSet
        self.__onDestroy = onDestroy
        self.__isDestroying = False
        self.__commands = list()
        self.__commands.append(self.__aliveCommand)
        self.__commands.append(self.__setCommand)
        self.__commands.append(self.__getLogCommand)

        separatedHost = ownHost.split(PORT_SEPARATOR)
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
                    self.__receiveMessage(pipe)
            self.__checkingLock.release()

    def __receiveMessage(self, pipe):
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

        