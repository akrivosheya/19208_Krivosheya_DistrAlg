from DistrHashMap import DistrHashMap

HOST = '192.168.56.1'
PORT = 4221
OTHER_PORT = 4222

m = DistrHashMap(HOST, PORT, HOST, OTHER_PORT)
input('1')
m.set('1')
#m.sendMessage(b"master 1")
input('2')
m.set('2')
#m.sendMessage(b"master 2")
input('3')
m.destroy()