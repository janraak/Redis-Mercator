#!/bin/python3
#
import sys
import os
import pathlib
from pathlib import Path
from subprocess import Popen
import redis
import json
import subprocess
import pdb

def which_modules_has_been_loaded(redis_client):
    module_tags = {}
    data = redis_client.execute_command("MODULE LIST")
    print(data)
    graphdb = False
    indexer = False
    query = False
    rules = False
    for m in data:
        print(m)
        if m[b'name'].decode('utf-8') == "rxGraphdb": module_tags["rxGraphdb"] = True
        if m[b'name'].decode('utf-8') == "rxMercator": module_tags["rxMercator"] = True
        if m[b'name'].decode('utf-8') == "rxIndexStore": module_tags["rxIndexStore"] = True
        if m[b'name'].decode('utf-8') == "rxIndexer": module_tags["rxIndexer"] = True
        if m[b'name'].decode('utf-8') == "RXQUERY": module_tags["rxQuery"] = True
        if m[b'name'].decode('utf-8') == "rxRule": module_tags["rxRule"] = True
    return module_tags

cid = sys.argv[1]
host = sys.argv[2]
port = sys.argv[3]
role = sys.argv[4]
ihost = sys.argv[5]
iport = sys.argv[6]
redis_version = "6.2.7" if len(sys.argv) < 8 else sys.argv[7]
cdn = "https://roxoft.dev/assets" if len(sys.argv) < 9 else sys.argv[8]
start_script = "__start_redis.sh" if len(sys.argv) < 10 else sys.argv[9]
install_script = "__install_rxmercator.sh" if len(sys.argv) < 11 else sys.argv[10]

print("cluster={}\n{}={}:{}\n~{}={}:{}\nversion={}\ncdn={}\nstart_script={}\ninstall_script={}\n".format(cid, role, host, port, role, ihost, iport, redis_version,cdn,start_script,install_script))
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

data_dir = "{}/data/{}".format(os.path.expanduser( '~' ),cid)
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

start0 = ''
# if node_is_local < 0:
#     start0 = "ssh {} ".format(host)

# os.system("{}rm {}/{}  {}/{} ".format(start0, home, start_script, home, install_script))
os.system("{}wget --no-check-certificate  --timestamping  -O {}/{} {}/{}".format(start0, home, start_script, cdn, start_script))
os.system("{}dos2unix {}/{}".format(start0, home, start_script))
os.system("{}wget --no-check-certificate  --timestamping  -O {}/{} {}/{}".format(start0, home, install_script, cdn, install_script))
os.system("{}dos2unix {}/{}".format(start0, home, install_script))

print ("{}bash  --debug --verbose  {}/{} {} {} {} 4 256 1GB {} >>{}/data/startup.log 2>>{}/data/startup.log".format(start0, home, start_script, redis_version, host, port, cid, wd, wd))
os.system("{}bash  --debug --verbose  {}/{} {} {} {} 4 256 1GB {}>>{}/data/startup.log 2>>{}/data/startup.log".format(start0, home, start_script, redis_version, host, port, cid, wd, wd))

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
        data =  redis_client.info("Persistence")
        if data["loading"] == 0:
            break
    except:
        pass

info = redis_client.info("SERVER");
segments = info["executable"].split('/')
path = '/'.join(segments[0:len(segments)-2])

print("Current db folder: {}".format(redis_client.execute_command("CONFIG GET DIR {}".format(wd))))
redis_client.execute_command("CONFIG SET DIR {}".format(data_dir))

if role == 'data':
    module_config = which_modules_has_been_loaded(redis_client)
    if not "rxIndexer" in module_config:
        print("MODULE LOAD {}/extensions/src/rxIndexer.so INDEX {} {} 0 DATA {} {} 0".format(path, ihost, iport, host, port))
        redis_client.execute_command("MODULE LOAD {}/extensions/src/rxIndexer.so INDEX {} {} 0 DATA {} {} 0".format(path, ihost, iport, host, port))
        print("rxIndexer loaded")
    else:
        print("rxIndexer already loaded")
    if not "rxQuery" in module_config:
        print("MODULE LOAD {}/extensions/src/rxQuery.so INDEX {} {} 0 DATA {} {} 0 DEFAULT_OPERATOR &".format(path, ihost, iport, host, port))
        redis_client.execute_command("MODULE LOAD {}/extensions/src/rxQuery.so INDEX {} {} 0 DATA {} {} 0 DEFAULT_OPERATOR &".format(path, ihost, iport, host, port))
        print("rxQuery loaded")
    else:
        print("rxQuery already loaded")
    if not "rxRule" in module_config:
        redis_client.execute_command("MODULE LOAD {}/extensions/src/rxRule.so INDEX {} {} 0 DATA {} {} 0 DEFAULT_OPERATOR & DEBUG".format(path, ihost, iport, host, port))
        print("rxRule loaded")
    else:
        print("rxRule already loaded")
    if not "rxGraphdb" in module_config:
        redis_client.execute_command("MODULE LOAD {}/extensions/src/rxGraphdb.so INDEX {} {} 0 DATA {} {} 0 DEFAULT_OPERATOR & DATA-DIR {}/data".format(path, ihost, iport, host, port, path))
        print("rxGraphdb loaded")
    else:
        print("rxGraphdb already loaded")
    if not "rxMercator" in module_config:
        redis_client.execute_command("MODULE LOAD {}/extensions/src/rxMercator.so CLIENT".format(path))
        print("rxMercator loaded")
    else:
        print("rxMercator already loaded")

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
elif role == 'master' or role == 'controller':
    module_config = which_modules_has_been_loaded(redis_client)
    if not "rxQuery" in module_config:
        print("MODULE LOAD {}/extensions/src/rxQuery.so".format(path))
        redis_client.execute_command("MODULE LOAD {}/extensions/src/rxQuery.so".format(path))
        print("rxQuery loaded")
    else:
        print("rxQuery already loaded")
    if not "rxRule" in module_config:
        print("MODULE LOAD {}/extensions/src/rxRule.so ".format(path))
        redis_client.execute_command("MODULE LOAD {}/extensions/src/rxRule.so ".format(path))
        print("rxRule loaded")
    else:
        print("rxRule already loaded")
    if not "rxMercator" in module_config:
        print("MODULE LOAD {}/extensions/src/rxMercator.so".format(path))
        redis_client.execute_command("MODULE LOAD {}/extensions/src/rxMercator.so".format(path))
        print("rxMercator loaded")
    else:
        print("rxMercator already loaded")
    # redis_client.execute_command("ACL SETUSER {} ON >{} +select|0".format(cid, cid))
    # redis_client.execute_command("ACL SETUSER {} -@admin".format(cid))
    # redis_client.execute_command("ACL SETUSER {} +@read".format(cid))
else:
    print(wd)
    print("MODULE LOAD {}/extensions/src/rxMercator.so CLIENT".format(path))
    module_config = which_modules_has_been_loaded(redis_client)
    if not "rxIndexStore" in module_config:
        redis_client.execute_command("MODULE LOAD {}/extensions/src/rxIndexStore.so INDEX {} {} 0 DATA {} {} 0 ".format(path, ihost, iport, host, port))
        print("rxIndexStore loaded")
    else:
        print("rxIndexStore already loaded")
    if not "rxMercator" in module_config:
        redis_client.execute_command("MODULE LOAD {}/extensions/src/rxMercator.so CLIENT".format(path))
        print("rxMercator loaded")
    else:
        print("rxMercator already loaded")
print(redis_client.execute_command("ACL SETUSER admin ON >admin +@all +@admin".format(cid, cid)))
print(redis_client.execute_command("ACL SETUSER admin ON >admin +@all +@admin".format(cid, cid)))
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

