from distrHashMap import DistrHashMap

HOST = '192.168.56.1:4223'
OTHER_HOSTS = ['192.168.56.1:4221', '192.168.56.1:4222']

m = DistrHashMap(HOST, OTHER_HOSTS)
input('1')
print("values: ", m.get('1'), ", ", m.get('2'))
input('2')
m.pop('2')
input('3')
m.set('4', 'no...')
input('4')
print(m.get('4'))
m.destroy()