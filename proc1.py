from DistrHashMap import AtomicOperations 

HOST = '192.168.56.1:4221'
OTHER_HOSTS = ['192.168.56.1:4222', '192.168.56.1:4223']

class Value:
    def __init__(self, value):
        self.value = value

cas = AtomicOperations(HOST, OTHER_HOSTS)
val = Value(0)
print(val.value)
input('wait')
for i in range(0, 10):
    cas.compareAndSwap(val, i, i + 1)
    print(val.value)
cas.destroy()