import sys
import os
import pathlib
from pathlib import Path
import time
from subprocess import Popen
import redis
import subprocess

host = sys.argv[1]
port = sys.argv[2]
role = sys.argv[3]
ihost = sys.argv[4]
iport = sys.argv[5]

node_is_local = str(subprocess.check_output('ifconfig')).find(host)

wd = pathlib.Path(sys.argv[0]).parent.parent.parent
base_fn = "{}.{}".format(host, port)


redis_server = ["src/redis-server", "--bind", "0.0.0.0", "--port", port, "--maxmemory", "1MB", 
    "--dbfilename", "{}.rdb".format(base_fn), 
    "--appendfilename", "{}.aof".format(base_fn), 
    "--logfile", "data/{}.log".format(base_fn), 
    "--databases", "4", "--maxclients", "256"]

if node_is_local < 0:
    redis_server.insert(0, host)
    redis_server.insert(0, "ssh")
    
pid = Popen(redis_server, cwd=str(wd), stdin=None, stdout=None, stderr=None)
print("Redis server started for {}:{} pid:{}".format(host,port, pid.pid))
redis_client = None
while True:
    try:
        redis_client = redis.StrictRedis(host, int(port), 0)
        break;
    except:
        pass

while True:
    try:
        data =  redis_client.execute_command("info Persistence").decode('utf-8')
        if "loading:0" in data:
            break
    except:
        pass

if role == 'data':
    redis_client.execute_command("MODULE LOAD extensions/src/rxIndexer.so {} {}".format(ihost, iport))
    redis_client.execute_command("MODULE LOAD extensions/src/rxQuery.so {} {} {} {}".format(ihost, iport, host, port))
    redis_client.execute_command("MODULE LOAD extensions/src/rxRule.so {} {} {} {}".format(ihost, iport, host, port))
    redis_client.execute_command("MODULE LOAD extensions/src/rxGraphdb.so {} {}".format(ihost, iport))
elif role == 'master':
    redis_client.execute_command("MODULE LOAD extensions/src/rxMercator.so")
else:
    redis_client.execute_command("MODULE LOAD extensions/src/rxIndexStore.so {} {}".format(host, port))

