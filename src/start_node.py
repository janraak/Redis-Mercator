#!/bin/python3
#
import sys
import os
import pathlib
from pathlib import Path
from subprocess import Popen
import redis
import subprocess
import pdb

cid = sys.argv[1]
host = sys.argv[2]
port = sys.argv[3]
role = sys.argv[4]
ihost = sys.argv[5]
iport = sys.argv[6]
redis_version = "7.0.5"

node_is_local = str(subprocess.check_output('ifconfig')).find(host)

print("a0: {}".format(sys.argv[0]))
print("a0: {}".format(pathlib.Path(sys.argv[0]).expanduser))
print("p0: {}".format(pathlib.Path(sys.argv[0]).parent))
print("p1: {}".format(pathlib.Path(sys.argv[0]).parent.parent))
print("p2: {}".format(pathlib.Path(sys.argv[0]).parent.parent.parent))
wd = pathlib.Path(sys.argv[0]).parent.parent.parent
home = str(os.environ.get('HOME')).replace('\n','')
print("{}:{}".format(type(home),home))
wd = "{}/redis-{}".format(home, redis_version)
base_fn = "{}.{}".format(host, port)
print("wd: {}".format(wd))
# # pdb.set_trace()

redis_server = ["src/redis-server", 
    "--daemonize", "yes", 
    "--bind", "0.0.0.0", 
    "--port", port, 
    "--maxmemory", "1MB", 
    "--dbfilename", "{}.rdb".format(base_fn), 
    "--dir", "{}/data".format(wd),
    "--appendfilename", "{}.aof".format(base_fn), 
    "--logfile", "{}/data/{}.log".format(wd, base_fn), 
    "--databases", "4", 
    "--maxclients", "256"]

start0 = ''
if node_is_local < 0:
    start0 = "ssh {} ".format(host)

os.system("cd{}rm {}/__start_redis.sh  {}/__install_rxmercator.sh ".format(start0, home, home))
os.system("{}wget -O {}/__start_redis.sh https://roxoft.dev/assets/__start_redis.sh".format(start0, home))
os.system("{}dos2unix {}/__start_redis.sh".format(start0, home))
os.system("{}wget -O {}/__install_rxmercator.sh https://roxoft.dev/assets/__install_rxmercator.sh".format(start0, home))
os.system("{}dos2unix {}/__install_rxmercator.sh".format(start0, home))

os.system("{}bash {}/__start_redis.sh {} {} {} 4 256 1GB >>{}/data/startup.log".format(start0, home, redis_version, host, port, wd))

# os.system("{}")rm 
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

path = '.'
info = str(redis_client.info("SERVER")).splitlines();
for l in info:
    parts = l.split(':')
    if parts[0] == 'executable':
        segments = parts[1].split('/')
        path = '/'.join(segments[0:len(segments)-2])

# # pdb.set_trace()
if role == 'data':
    redis_client.execute_command("MODULE LOAD {}/extensions/src/rxMercator.so CLIENT".format(wd))
    ml = "MODULE LOAD {}/extensions/src/rxIndexer.so INDEX {} {} 0 DATA {} {} 0 DEFAULT_OPERATOR &".format(wd, ihost, iport, host, port)
    r = redis_client.execute_command(ml)
    redis_client.execute_command("MODULE LOAD {}/extensions/src/rxQuery.so INDEX {} {} 0 DATA {} {} 0 DEFAULT_OPERATOR &".format(wd, ihost, iport, host, port))
    redis_client.execute_command("MODULE LOAD {}/extensions/src/rxRule.so INDEX {} {} 0 DATA {} {} 0 DEFAULT_OPERATOR &".format(wd, ihost, iport, host, port))
    redis_client.execute_command("MODULE LOAD {}/extensions/src/rxGraphdb.so INDEX {} {} 0 DATA {} {} 0 DEFAULT_OPERATOR &".format(wd, ihost, iport, host, port))
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
    redis_client.execute_command("MODULE LOAD {}/extensions/src/rxMercator.so".format(wd))
    # redis_client.execute_command("ACL SETUSER admin ON >admin +@all +@admin".format(cid, cid))
    # redis_client.execute_command("ACL SETUSER admin ON >admin +@all +@admin".format(cid, cid))
    # redis_client.execute_command("ACL SETUSER {} ON >{} +select|0".format(cid, cid))
    # redis_client.execute_command("ACL SETUSER {} -@admin".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@read".format(cid))
else:
    print(wd)
    print("MODULE LOAD {}/extensions/src/rxMercator.so CLIENT".format(wd))
    redis_client.execute_command("MODULE LOAD {}/extensions/src/rxMercator.so CLIENT".format(wd))
    redis_client.execute_command("MODULE LOAD {}/extensions/src/rxIndexStore.so INDEX {} {} 0 DATA {} {} 0 ".format(wd, ihost, iport, host, port))
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

