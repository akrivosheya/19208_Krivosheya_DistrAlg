from distrHashMap import DistrHashMap

HOST = '192.168.56.1:4222'
OTHER_HOSTS = ['192.168.56.1:4221']

m = DistrHashMap(HOST, OTHER_HOSTS)
input('1')
print("values: ", m.get('1'), ", ", m.get('2'))
input('2')
m.destroy()