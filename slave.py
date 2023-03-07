from DistrHashMap import DistrHashMap

HOST = '192.168.56.1'
PORT = 4222
OTHER_PORT = 4221

m = DistrHashMap(HOST, PORT, HOST, OTHER_PORT)
input('1')
print("values: ", m.get('1'), ", ", m.get('2'))
input('2')
m.destroy()