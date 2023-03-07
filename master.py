from distrHashMap import DistrHashMap

HOST = '192.168.56.1:4221'
OTHER_HOSTS = ['192.168.56.1:4222']

m = DistrHashMap(HOST, OTHER_HOSTS)
input('1')
m.set('1')
#m.sendMessage(b"master 1")
input('2')
m.set('2')
#m.sendMessage(b"master 2")
input('3')
m.destroy()