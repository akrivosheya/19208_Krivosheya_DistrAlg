import threading
import time

from failureDetection import FailureDetector
from connections import PerfectLink

LEADER = 0
FOLLOWER = 1
CANDIDATE = 2

HEART_BEAT_COMMAND = 0
SET_COMMAND = 1

SLEEP_TIME = 1
FAILURE_DETECTION_TIMES = 5

SESSION_INDEX = 1

PORT_SEPARATOR = ':'

class RaftNode:
    def __init__(self, ownHost, otherHosts, onRemoteSet, onDestroy):
        self.__onRemoteSet = onRemoteSet
        self.__onDestroy = onDestroy
        self.__isDestroying = False

        self.__currentSession = 1
        self.__votedFor = None
        self.__logs = list()
        self.__commitIndex = 0
        self.__lastApplied = 0
        self.__ownNode = self.__getParsedHost(ownHost)
        self.__allNodes = list()
        self.__nextIndex = dict()
        self.__matchIndex = dict()
        for otherHost in otherHosts:
            nodeData = self.__getParsedHost(otherHost)
            self.__allNodes.append(nodeData)
            self.__nextIndex[nodeData] = 1
            self.__matchIndex[nodeData] = 0

        self.__commands = list()
        self.__commands.append(self.__heartBeatCommand)
        self.__commands.append(self.__setCommand)

        self.__failureDetector = FailureDetector(self.__allNodes)
        self.__failureDetector.setIsAlive(self.__ownNode, True)

        self.__perfectLink = PerfectLink(self.__ownNode, self.__allNodes, onDestroy)

        self.__allNodes.append(self.__ownNode)
        self.__leader = self.__getLeader()
        if(self.__leader == self.__ownNode):
            self.__role = LEADER
        else:
            self.__role = FOLLOWER

        self.__onTickThread = threading.Thread(target=self.__doOnTick)
        self.__onTickThread.start()

    def destroy(self):
        self.__isDestroying = True
        self.__perfectLink.destroy()
        self.__onTickThread.join()

    def setRPC(self, key, value):
        if(self.__role == FOLLOWER):
            pass
            self.__perfectLink.sendMessageTo(SET_COMMAND, (key, value), None, None, None, None, None)
        if(self.__role == LEADER):
            self.__onRemoteSet(key, value)#нужен коммит
            self.__logs.append((SET_COMMAND, self.__currentSession, (key, value)))
            self.__perfectLink.sendBroadcastMessage(SET_COMMAND, (key, value), self.__currentSession, 
                                                    len(self.__logs), (self.__logs[len(self.__logs) - 1])[SESSION_INDEX], 
                                                    self.__commitIndex)

    def __doOnTick(self):
        iterations = 0
        while True:
            time.sleep(SLEEP_TIME)
            if(self.__isDestroying):
                return
            messages = self.__perfectLink.getMessages()
            self.__checkMessages(messages)
            if(self.__role == LEADER):#функция
                if(len(self.__logs) == 0):
                    prevLogSession = self.__currentSession
                else:
                    prevLogSession = (self.__logs[len(self.__logs) - 1])[SESSION_INDEX]
                self.__perfectLink.sendBroadcastMessage(HEART_BEAT_COMMAND, None, self.__currentSession, 
                                                        len(self.__logs), prevLogSession, 
                                                        self.__commitIndex)
            iterations += 1
            if(iterations == FAILURE_DETECTION_TIMES):
                self.__failureDetector.checkNodes()
                print('leader ', self.__failureDetector.nodeIsAlive(self.__leader))
            

    def __checkMessages(self, messages):
        for message in messages:
            (messageType, session, nodeData, prevLogIndex, prevLogSession, leaderCommit, data) = message
            self.__failureDetector.setIsAlive(nodeData, True)
            self.__commands[messageType](messageType, session, nodeData, data)

    def __heartBeatCommand(self, messageType, session, nodeData, data):
        self.__failureDetector.setIsAlive(data, True)
        if(self.__role == FOLLOWER):
            if(len(self.__logs) == 0):
                prevLogSession = self.__currentSession
            else:
                prevLogSession = (self.__logs[len(self.__logs) - 1])[SESSION_INDEX]
            self.__perfectLink.sendMessageTo(messageType, self.__ownNode, self.__leader, self.__currentSession,
                                                        len(self.__logs), prevLogSession, 
                                                        self.__commitIndex)
        #всякие штуки с логом

    def __setCommand(self, messageType, session, nodeData, data):#нужно будет переделать и присваивать при команде коммит
        print("set ", data)
        key, value = data
        self.__onRemoteSet(key, value)
        self.__logs.append((messageType, session, data))
        if(self.__role == LEADER):
            self.__perfectLink.sendBroadcastMessage(messageType, data, session)

    def __getLeader(self):#проверки + одинаковые порты
        leader = self.__allNodes[0]
        print(leader)
        for host in self.__allNodes[1:len(self.__allNodes)]:
            print(host, ' ', leader)
            if host[1] < leader[1]:
                leader = host
        print('leader ', leader)
        return leader
    
    def __getParsedHost(self, host):
        separatedHost = host.split(PORT_SEPARATOR)
        return (separatedHost[0], int(separatedHost[1]))