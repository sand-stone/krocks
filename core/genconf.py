nodes = ['10.0.0.7', '10.0.0.8', '10.0.0.9']
dataports = [8000, 8000, 8000]
replports = [5000, 5000, 5000]
numrings = 10
logDir = 'log'
dataDir = 'data/'

for i in range(len(nodes)):
    f = open('acme%s.conf' % i, 'w')
    f.write('serverid=%s\n'%i)
    f.write('port=%s\n' % dataports[i])
    f.write('store=%s%s\n' % (dataDir, i))
    port = replports[i]
    for j in range(numrings):
        f.write("ringaddr=%s:%s\n" % (nodes[i], port))
        if i > 0:
            f.write("leader=%s:%s\n" % (nodes[i-1], replports[i-1]+j))
        f.write("logDir=%s/server%s-%s\n" % (logDir, i, j))
        port = port + 1
    f.close()
