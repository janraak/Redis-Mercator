import sys
import os
import pathlib
from pathlib import Path
import time
from subprocess import Popen
import redis
import subprocess
import os

cid = sys.argv[1]
host = sys.argv[2]
port = sys.argv[3]
role = sys.argv[4]
ihost = sys.argv[5]
iport = sys.argv[6]

node_is_local = str(subprocess.check_output('ifconfig')).find(host)

# pdb.set_trace()
wd = os.getcwd()  #pathlib.Path(sys.argv[0]).parent.parent.parent.joinpath("")
base_fn = "{}.{}".format(host, port)


redis_server = ["src/redis-server", "--bind", "0.0.0.0", "--port", port, "--maxmemory", "1MB", 
    "--dbfilename", "{}.rdb".format(base_fn), 
    "--dir", "{}/data".format(wd), 
    "--appendfilename", "{}.aof".format(base_fn), 
    "--logfile", "{}/data/{}.log".format(wd, base_fn), 
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
    redis_client.execute_command("MODULE LOAD extensions/src/rxIndexer.so {} {} {} {}".format(ihost, iport, host, port))
    redis_client.execute_command("MODULE LOAD extensions/src/rxQuery.so {} {} {} {}  INDEX {} {} 0 DATA {} {} 0 DEFAULT_OPERATOR &".format(ihost, iport, host, port, ihost, iport, host, port))
    redis_client.execute_command("MODULE LOAD extensions/src/rxRule.so {} {} {} {}".format(ihost, iport, host, port))
    redis_client.execute_command("MODULE LOAD extensions/src/rxGraphdb.so {} {} {} {}".format(ihost, iport, host, port))
    # redis_client.execute_command("ACL SETUSER admin ON >admin +@all +@admin".format(cid, cid))
    # redis_client.execute_command("ACL SETUSER {} ON >{} +select|0".format(cid, cid))
    # redis_client.execute_command("ACL SETUSER {} -@admin".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@read".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@write".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@keyspace".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@hash".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@set".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@sortedset".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@list".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@bitmap".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@hyperloglog".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@geo".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@stream".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@pubsub".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@transaction".format(cid))
elif role == 'master':
    redis_client.execute_command("MODULE LOAD extensions/src/rxMercator.so")
    # redis_client.execute_command("ACL SETUSER admin ON >admin +@all +@admin".format(cid, cid))
    # redis_client.execute_command("ACL SETUSER admin ON >admin +@all +@admin".format(cid, cid))
    # redis_client.execute_command("ACL SETUSER {} ON >{} +select|0".format(cid, cid))
    # redis_client.execute_command("ACL SETUSER {} -@admin".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@read".format(cid))
else:
    redis_client.execute_command("MODULE LOAD extensions/src/rxIndexStore.so {} {}".format(host, port))
    # redis_client.execute_command("ACL SETUSER {} ON >{} +select|0".format(cid, cid))
    # redis_client.execute_command("ACL SETUSER {} -@admin".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@read".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@write".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@keyspace".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@hash".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@set".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@sortedset".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@list".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@bitmap".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@hyperloglog".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@geo".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@stream".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@pubsub".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@transaction".format(cid))

