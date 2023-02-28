import socket
import select
import threading
import time

CHECKING_WAIT_TIME = 0.5
BUF_SIZE = 1024

class DistrHashMap:
    def __init__(self, host, port, other_host, other_port):#проверки
        self.host = host
        self.port = port
        self.other_host = other_host
        self.other_port = other_port
        self.descriptors = set()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)#exceptions!!!
        if (port < other_port):
            self.socket.bind((host, port))
            self.socket.listen(1)
            self.connection, self.address = self.socket.accept()
            self.descriptors.add(self.connection.fileno())
            self.message_pipe = self.connection
        else:
            self.socket.connect((other_host, other_port))
            self.descriptors.add(self.socket.fileno())
            self.message_pipe = self.socket
        self.checking_cycle = threading.Thread(target=self.check_messages)
        self.is_destroying = False
        self.checking_cycle.start()

    def destroy(self):
        self.is_destroying = True
        self.checking_cycle.join()

    def send_message(self, mes):
        self.message_pipe.sendall(mes)#exceptions!!!

    def recieve_message(self):
        data = self.message_pipe.recv(BUF_SIZE)#exceptions!!!
        if (data.__len__() > 0):
            print(data)

    def select(self):
        rlist, wlist, xlist = select.select(list(self.descriptors), list(self.descriptors), list(self.descriptors), 1)
        allDescrs = set(rlist + wlist + xlist)
        rlist = set(rlist)
        wlist = set(wlist)
        xlist = set(xlist)
        for descriptors in allDescrs:
            canRead = False
            if descriptors in rlist:
                canRead = True
            """
            if descr in wlist:
                event |= 2
            if descr in xlist:
                event |= 4
            """
        if canRead:
            self.recieve_message()

    def check_messages(self):
        while True:
            time.sleep(CHECKING_WAIT_TIME)
            if(self.is_destroying):
                break
            self.select()
        


