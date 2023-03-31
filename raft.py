import threading
import time
import random

from failureDetection import FailureDetector
from connections import PerfectLink

LEADER = 0
FOLLOWER = 1
CANDIDATE = 2

ALIVE_RESPOND = 0
SET_COMMAND = 1
APPEND_ENTRIES_COMMAND = 2
RESPOND = 3
VOTE_COMMAND = 4
VOTE_RESPOND = 5

SLEEP_TIME = 1
FAILURE_DETECTION_TIMES = 5
MAX_ELECTION_TIMEOUT = 10
MIN_ELECTION_TIMEOUT = 7

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
        self.__voteResults = dict()
        self.__logs = list()
        self.__commitIndex = 0
        self.__lastApplied = 0
        self.__minMatchIndex = 0
        self.__ownNode = self.__getParsedHost(ownHost)
        self.__allNodes = list()
        self.__nextIndex = dict()
        self.__matchIndex = dict()
        self.__voteResults = dict()
        self.__electionTimeout = 0
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
        self.__commands.append(self.__voteCommand)
        self.__commands.append(self.__voteRespond)

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
        self.__electionTimeout = random.randint(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
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
            elif(self.__role == CANDIDATE):
                self.__candidateDoOnTick()
            iterations += 1
            self.__electionTimeout -= 1
            if(iterations == FAILURE_DETECTION_TIMES):
                iterations = 0
                self.__failureDetector.checkNodes()
            if(self.__electionTimeout == 0):
                self.__electionTimeout = random.randint(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
                if(self.__role == CANDIDATE or (not self.__failureDetector.nodeIsAlive(self.__leader))):
                    self.__role = CANDIDATE
                    self.__votedFor = self.__ownNode
                    self.__currentSession += 1
                    for node in self.__allNodes:
                        self.__voteResults[node] = False
                    self.__voteResults[self.__ownNode] = True

    def __applyLog(self, log):
        if(log[TYPE_INDEX] == SET_COMMAND):
            key, value = log[DATA_INDEX]
            self.__onRemoteSet(key, value)
            
    def __leaderDoOnTick(self):
        indexes = [self.__matchIndex[i] for i in self.__allNodes if (i != self.__ownNode and self.__failureDetector.nodeIsAlive(i))]
        if(len(indexes) == 0):
            minIndex = 0
        else:
            minIndex = min(indexes)
        self.__minMatchIndex = minIndex
        if(self.__minMatchIndex > 0 and (self.__logs[self.__minMatchIndex - 1])[SESSION_INDEX] == self.__currentSession):
            self.__commitIndex = self.__minMatchIndex
        for node in self.__allNodes:
            if(node == self.__ownNode):
                continue
            nextIndex = self.__nextIndex[node]
            lenLogs = len(self.__logs)
            if(nextIndex == 1):
                prevLogSession = 0
            else:
                prevLogSession = (self.__logs[nextIndex - 2])[SESSION_INDEX]
            if(lenLogs == nextIndex - 1):
                entries = list()
            else:
                entries = self.__logs[nextIndex - 1]
            """
            if(lenLogs == nextIndex - 1 or nextIndex < 1):
                entries = list()
                prevLogSession = self.__currentSession
            else:
                try:
                    entries = self.__logs[nextIndex - 1]
                except:
                    print('bad ', lenLogs, ' ', nextIndex - 1)
                if(nextIndex - 2 >= 0):
                    prevLogSession = (self.__logs[nextIndex - 2])[SESSION_INDEX]
                else:
                    prevLogSession = self.__currentSession
            """
            self.__perfectLink.sendMessageTo(APPEND_ENTRIES_COMMAND, entries, node, self.__currentSession,
                nextIndex - 1, prevLogSession, self.__commitIndex)
            
    def __candidateDoOnTick(self):
        for node in self.__allNodes:
            if(node == self.__ownNode):
                continue
            prevLogIndex = len(self.__logs)
            if(prevLogIndex <= 1):
                prevLogSession = self.__currentSession
            else:
                prevLogSession = (self.__logs[prevLogIndex - 1])[SESSION_INDEX]
            self.__perfectLink.sendMessageTo(VOTE_COMMAND, self.__ownNode, node, self.__currentSession,
                prevLogIndex, prevLogSession, self.__commitIndex)

    def __checkMessages(self, messages):
        for message in messages:
            print('mes ', message)
            (messageType, session, nodeData, prevLogIndex, prevLogSession, leaderCommit, data) = message
            if(session > self.__currentSession and (messageType == APPEND_ENTRIES_COMMAND or messageType == VOTE_COMMAND)):
                self.__currentSession = session
                self.__votedFor = None
                self.__role = FOLLOWER
                self.__leader = nodeData
            """
            if(session < self.__currentSession and self.__role != CANDIDATE):
                continue
            """
            self.__failureDetector.setIsAlive(nodeData, True)
            self.__commands[messageType](messageType, session, nodeData, data, prevLogIndex, prevLogSession, leaderCommit)

    def __aliveRespond(self, messageType, session, nodeData, data, prevLogIndex, prevLogSession, leaderCommit):
        #print('alive respond')
        self.__failureDetector.setIsAlive(data, True)

    def __setCommand(self, messageType, session, nodeData, data, prevLogIndex, prevLogSession, leaderCommit):#нужно будет переделать и присваивать при команде коммит
        print("set request ", data)
        if(self.__role == LEADER):
            self.__logs.append((SET_COMMAND, self.__currentSession, data))
            print('leader logs ', self.__logs)

    def __appendEntriesCommand(self, messageType, session, nodeData, data, prevLogIndex, prevLogSession, leaderCommit):
        if(self.__role == FOLLOWER):
            self.__electionTimeout = random.randint(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
            success = True
            if(session < self.__currentSession):
                success = False
            elif(len(self.__logs) <= prevLogIndex - 1):
                success = False
            else:
                if(prevLogIndex == 0):
                    self.__logs.clear()
                elif(prevLogIndex > 0):
                    if(self.__logs[prevLogIndex - 1][SESSION_INDEX] != prevLogSession):
                        removeUntilIndex = prevLogIndex - 1
                    else:
                        removeUntilIndex = prevLogIndex
                    self.__logs = [self.__logs[i] for i in range(0, removeUntilIndex)]
                if(len(data) != 0):
                    self.__logs.append(data)
                    print('logs ', self.__logs)
                if(leaderCommit > self.__commitIndex):
                    self.__commitIndex = min(leaderCommit, len(self.__logs))
            """
            if(len(data) == 0):
                self.__perfectLink.sendMessageTo(ALIVE_RESPOND, success, self.__leader, self.__currentSession, NONE_NUMBER, NONE_NUMBER, NONE_NUMBER)
                if(leaderCommit > self.__commitIndex):
                    self.__commitIndex = min(leaderCommit, len(self.__logs))
                return
            if(session < self.__currentSession or len(self.__logs) <= prevLogIndex - 1):
                success = False
            elif(prevLogIndex > 0 and self.__logs[prevLogIndex - 1][SESSION_INDEX] != self.__currentSession):
                self.__logs = [self.__logs[i] for i in range(0, prevLogIndex - 1)]
            elif(prevLogIndex == 0):
                self.__logs.clear()
            if(success):
                self.__logs.append(data)
                if(leaderCommit > self.__commitIndex):
                    self.__commitIndex = min(leaderCommit, len(self.__logs))
            """
            self.__perfectLink.sendMessageTo(RESPOND, success, self.__leader, self.__currentSession, prevLogIndex, NONE_NUMBER, NONE_NUMBER)

    def __respond(self, messageType, session, nodeData, success, prevLogIndex, prevLogSession, leaderCommit):
        print('success ', success)
        if(self.__role == LEADER and self.__nextIndex[nodeData] == prevLogIndex + 1):
            if success:
                self.__nextIndex[nodeData] += 1
                self.__matchIndex[nodeData] += 1
                if(self.__nextIndex[nodeData] > len(self.__logs) + 1):
                    self.__nextIndex[nodeData] = len(self.__logs) + 1
                    self.__matchIndex[nodeData] = len(self.__logs)
            else:
                self.__nextIndex[nodeData] -= 1
                self.__matchIndex[nodeData] -= 1
                if(self.__nextIndex[nodeData] <= 0):
                    self.__nextIndex[nodeData] = 1
                    self.__matchIndex[nodeData] = 0

    def __voteCommand(self, messageType, session, nodeData, success, prevLogIndex, prevLogSession, leaderCommit):
        if(self.__role == FOLLOWER):
            self.__electionTimeout = random.randint(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
            success = False
            if(session >= self.__currentSession and (self.__votedFor == None or self.__votedFor == nodeData)):
                if(prevLogIndex >= len(self.__logs) and prevLogSession >= self.__logs[len(self.__logs) - 1][SESSION_INDEX]):
                    self.__voteFor = nodeData
                    success = True
                    self.__perfectLink.sendMessageTo(VOTE_RESPOND, True, nodeData, self.__currentSession, NONE_NUMBER, NONE_NUMBER, NONE_NUMBER)
                    return
        self.__perfectLink.sendMessageTo(VOTE_RESPOND, success, nodeData, self.__currentSession, NONE_NUMBER, NONE_NUMBER, NONE_NUMBER)
        return
    
    def __voteRespond(self, messageType, session, nodeData, success, prevLogIndex, prevLogSession, leaderCommit):
        if(self.__role == CANDIDATE):
            if success:
                self.__voteResults[nodeData] = True
                if(self.__wonVote()):
                    self.__role = LEADER
                    self.__leader = self.__ownNode
            else:
                self.__voteResults[nodeData] = False

    
    def __wonVote(self):
        aliveNodes = 0
        votedNodes = 0
        for node, voted in self.__voteResults.items():
            if(self.__failureDetector.nodeIsAlive(node)):
                aliveNodes += 1
            if(self.__voteResults[node]):
                votedNodes += 1
        print(votedNodes > (aliveNodes / 2), " ", votedNodes, aliveNodes)
        return votedNodes > (aliveNodes / 2)

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