from DistrHashMap import DistrHashMap

HOST = '192.168.56.1:4222'
OTHER_HOSTS = ['192.168.56.1:4221', '192.168.56.1:4223']

m = DistrHashMap(HOST, OTHER_HOSTS)
input('1')
print("values: ", m.get('1'), ", ", m.get('2'))
input('2')
m.set('4', 'lol')
input('3')
print(m.get('4'))
m.destroy()