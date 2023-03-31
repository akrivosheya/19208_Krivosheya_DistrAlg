class FailureDetector:
    def __init__(self, allNodes):
        self.__aliveBeforeCheckingNodes = dict()
        self.__aliveAfterCheckingNodes = dict()
        for node in allNodes:
            self.__aliveBeforeCheckingNodes[node] = False
            self.__aliveAfterCheckingNodes[node] = True

    def setIsAlive(self, node, isAlive):#проверки
        self.__aliveBeforeCheckingNodes[node] = isAlive

    def nodeIsAlive(self, node):#проверки
        return self.__aliveAfterCheckingNodes[node]

    def checkNodes(self):
        for node, isAlive in self.__aliveBeforeCheckingNodes.items():
            if isAlive:
                self.__aliveAfterCheckingNodes[node] = True
            else:
                self.__aliveAfterCheckingNodes[node] = False
            self.__aliveBeforeCheckingNodes[node] = False