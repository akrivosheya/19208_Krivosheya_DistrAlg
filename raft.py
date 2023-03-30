import threading
import time

from failureDetection import FailureDetector
from connections import PerfectLink

LEADER = 0
FOLLOWER = 1
CANDIDATE = 2

ALIVE_RESPOND = 0
SET_COMMAND = 1
APPEND_ENTRIES_COMMAND = 2
RESPOND = 3

SLEEP_TIME = 1
FAILURE_DETECTION_TIMES = 5

TYPE_INDEX = 0
SESSION_INDEX = 1
DATA_INDEX = 2

NONE_NUMBER = 0
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
        self.__minMatchIndex = 0
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
        self.__commands.append(self.__aliveRespond)
        self.__commands.append(self.__setCommand)
        self.__commands.append(self.__appendEntriesCommand)
        self.__commands.append(self.__respond)

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

    def setRPC(self, key, value):#нужен respond
        if(self.__role == FOLLOWER):
            pass
            self.__perfectLink.sendMessageTo(SET_COMMAND, (key, value), self.__leader, self.__currentSession, NONE_NUMBER, NONE_NUMBER, NONE_NUMBER)
        if(self.__role == LEADER):
            #self.__onRemoteSet(key, value)
            self.__logs.append((SET_COMMAND, self.__currentSession, (key, value)))
            """
            self.__perfectLink.sendBroadcastMessage(SET_COMMAND, (key, value), self.__currentSession, 
                                                    len(self.__logs), (self.__logs[len(self.__logs) - 1])[SESSION_INDEX], 
                                                    self.__commitIndex)
            """

    def __doOnTick(self):
        iterations = 0
        while True:
            time.sleep(SLEEP_TIME)
            if(self.__isDestroying):
                return
            self.__failureDetector.setIsAlive(self.__ownNode, True)
            messages = self.__perfectLink.getMessages()
            self.__checkMessages(messages)
            if(self.__commitIndex > self.__lastApplied):
                for i in range(self.__lastApplied, len(self.__logs)):
                    self.__applyLog(self.__logs[i])
                self.__lastApplied = self.__commitIndex
                print('applied')
            if(self.__role == LEADER):#функции в списке
                self.__leaderDoOnTick()
            iterations += 1
            if(iterations == FAILURE_DETECTION_TIMES):
                iterations = 0
                self.__failureDetector.checkNodes()
                print('leader ', self.__failureDetector.nodeIsAlive(self.__leader))#стать кандидатом

    def __applyLog(self, log):
        if(log[TYPE_INDEX] == SET_COMMAND):
            key, value = log[DATA_INDEX]
            self.__onRemoteSet(key, value)
            
    def __leaderDoOnTick(self):
        minIndex = min([self.__matchIndex[i] for i in self.__allNodes if (i != self.__ownNode and self.__failureDetector.nodeIsAlive(i))])
        self.__minMatchIndex = minIndex
        if(self.__minMatchIndex > 0 and (self.__logs[self.__minMatchIndex - 1])[SESSION_INDEX] == self.__currentSession):
            self.__commitIndex = self.__minMatchIndex
        for node in self.__allNodes:
            if(node == self.__ownNode):
                continue
            nextIndex = self.__nextIndex[node]
            lenLogs = len(self.__logs)
            if(lenLogs == nextIndex - 1):
                entries = list()
                prevLogSession = self.__currentSession
            else:
                entries = self.__logs[nextIndex - 1]
                prevLogSession = (self.__logs[nextIndex - 2])[SESSION_INDEX]
            self.__perfectLink.sendMessageTo(APPEND_ENTRIES_COMMAND, entries, node, self.__currentSession,
                nextIndex - 1, prevLogSession, self.__commitIndex)

    def __checkMessages(self, messages):
        for message in messages:
            (messageType, session, nodeData, prevLogIndex, prevLogSession, leaderCommit, data) = message
            if(session > self.__currentSession):
                self.__currentSession = session
                self.__role = FOLLOWER
            if(session < self.__currentSession):
                continue
            self.__failureDetector.setIsAlive(nodeData, True)
            self.__commands[messageType](messageType, session, nodeData, data, prevLogIndex, prevLogSession, leaderCommit)

    def __aliveRespond(self, messageType, session, nodeData, data, prevLogIndex, prevLogSession, leaderCommit):
        #print('alive respond')
        self.__failureDetector.setIsAlive(data, True)

    def __setCommand(self, messageType, session, nodeData, data, prevLogIndex, prevLogSession, leaderCommit):#нужно будет переделать и присваивать при команде коммит
        print("set request ", data)
        if(self.__role == LEADER):
            self.__logs.append((SET_COMMAND, self.__currentSession, data))

    def __appendEntriesCommand(self, messageType, session, nodeData, data, prevLogIndex, prevLogSession, leaderCommit):
        if(self.__role == FOLLOWER):
            success = True
            if(len(data) == 0):
                self.__perfectLink.sendMessageTo(ALIVE_RESPOND, success, self.__leader, self.__currentSession, NONE_NUMBER, NONE_NUMBER, NONE_NUMBER)
                if(leaderCommit > self.__commitIndex):
                    self.__commitIndex = min(leaderCommit, len(self.__logs))
                return
            if(session < self.__currentSession or len(self.__logs) <= prevLogIndex - 1):
                success = False
            elif(prevLogIndex > 0 and self.__logs[prevLogIndex - 1][SESSION_INDEX] != self.__currentSession):
                self.__logs = [self.__logs[i] for i in range(0, prevLogIndex - 1)]
            if(success):
                self.__logs.append(data)
                if(leaderCommit > self.__commitIndex):
                    self.__commitIndex = min(leaderCommit, len(self.__logs))
            print('logs ', self.__logs)
            self.__perfectLink.sendMessageTo(RESPOND, success, self.__leader, self.__currentSession, NONE_NUMBER, NONE_NUMBER, NONE_NUMBER)
        pass

    def __respond(self, messageType, session, nodeData, success, prevLogIndex, prevLogSession, leaderCommit):
        #print('success ', success)
        if success:
            self.__nextIndex[nodeData] += 1
            self.__matchIndex[nodeData] += 1
        else:
            self.__nextIndex[nodeData] -= 1#проверки на отрицательность
            self.__matchIndex[nodeData] -= 1
            if(self.__nextIndex[nodeData] <= 0):
                self.__nextIndex[nodeData] = 1
                self.__matchIndex[nodeData] = 0


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