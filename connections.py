import socket
import select
import pickle
import threading
import time

MESSAGE_TYPE_SIZE = 1
MESSAGE_LENGTH_SIZE = 1
MESSAGE_TERM_SIZE = 1
INDEX_SIZE = 1
BUFFER_SIZE_LIMIT = 100

CONNECT_TIME = 1
SELECT_TIME = 1
ACCEPTING_TIMEOUT = 5
MAX_ATTEMPTS = 5

class PerfectLink:
    def __init__(self, ownHost, otherHosts, onDestroy):
        self.__ownNode = ownHost
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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

    def sendBroadcastMessage(self, messageType, value, term, prevLogIndex, prevLogTerm, leaderCommit):
        for node in self.__writeConnections.keys():
            if(node == self.__ownNode):
                continue
            self.sendMessageTo(messageType, value, node, term, prevLogIndex, prevLogTerm, leaderCommit)

    def sendMessageTo(self, messageType, value, node, term, prevLogIndex, prevLogTerm, leaderCommit):
        serializedTerm = term.to_bytes(MESSAGE_TERM_SIZE, byteorder='little', signed=False)
        serializedNode = pickle.dumps(self.__ownNode)
        nodeLength = serializedNode.__len__().to_bytes(MESSAGE_LENGTH_SIZE, byteorder='little', signed=False)
        serializedValue = pickle.dumps(value)
        length = serializedValue.__len__().to_bytes(MESSAGE_LENGTH_SIZE, byteorder='little', signed=False)
        serializedMessageType = messageType.to_bytes(MESSAGE_TYPE_SIZE, byteorder='little', signed=False)
        serializedPrevLog = prevLogIndex.to_bytes(INDEX_SIZE, byteorder='little', signed=False)
        serializedPrevLogTerm = prevLogTerm.to_bytes(INDEX_SIZE, byteorder='little', signed=False)
        serializedLeaderCommit = leaderCommit.to_bytes(INDEX_SIZE, byteorder='little', signed=False)
        message = (b'').join([serializedMessageType, serializedTerm, nodeLength, serializedNode, 
                              serializedPrevLog, serializedPrevLogTerm, serializedLeaderCommit, length, serializedValue])
        if(self.__writeConnections[node] == None):
            connection = self.__tryToConnect(node)
            if(connection == None):
                #print('no connection ', node)
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
            time.sleep(SELECT_TIME)
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
                #print('no accept ', ex)
                pass

    def __receiveMessage(self, socket):
        try:
            socket.setblocking(False)
            serializedMessageType = socket.recv(MESSAGE_TYPE_SIZE)
            if serializedMessageType.__len__() > 0:
                messageType = int.from_bytes(serializedMessageType, byteorder='little', signed=False)
                serializedTerm = socket.recv(MESSAGE_TERM_SIZE)
                term = int.from_bytes(serializedTerm, byteorder='little', signed=False)
                serializedNodeLength = socket.recv(MESSAGE_LENGTH_SIZE)
                nodeLength = int.from_bytes(serializedNodeLength, byteorder='little', signed=False)
                serializedNodeData = socket.recv(nodeLength)
                nodeData = pickle.loads(serializedNodeData)
                serializedPrevLogIndex = socket.recv(INDEX_SIZE)
                prevLogIndex = int.from_bytes(serializedPrevLogIndex, byteorder='little', signed=False)
                serializedPrevLogTerm = socket.recv(INDEX_SIZE)
                prevLogTerm = int.from_bytes(serializedPrevLogTerm, byteorder='little', signed=False)
                serializedLeaderCommit = socket.recv(INDEX_SIZE)
                leaderCommit = int.from_bytes(serializedLeaderCommit, byteorder='little', signed=False)
                serializedLength = socket.recv(MESSAGE_LENGTH_SIZE)
                length = int.from_bytes(serializedLength, byteorder='little', signed=False)
                serializedData = socket.recv(length)
                data = pickle.loads(serializedData)
                self.__bufferLock.acquire()
                self.__buffer.append((messageType, term, nodeData, prevLogIndex, prevLogTerm, leaderCommit, data))
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
            #print('connected to ', node)
            return newSocket
        else:
            #print("Can't connect to ", node)
            newSocket.close()
            return None
        
    def __closeConnections(self, connections):
        hosts = list(connections.keys())
        for host in hosts:
            socket = connections.pop(host)
            if socket != None:
                socket.close()

        