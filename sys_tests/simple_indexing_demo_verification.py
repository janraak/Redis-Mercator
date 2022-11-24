import redis
import pdb
import traceback
import dataset_family
import time
import sys

from os.path import abspath
import os.path
from pathlib import Path

def check_server(must_flush):

    print ('Number of arguments: {} arguments.'.format(len(sys.argv))) 
    print ('Argument List: {}'.format( str(sys.argv)))
    filename = abspath(sys.argv[0])
    print(filename)
    path = Path(sys.argv[0])
    modulePath = path.parent.parent.joinpath("src").absolute()
    print(modulePath)

    redis_client = redis.StrictRedis('192.168.1.180', 6400, 0)

    while True:
        data =  execute(-1,redis_client,"info Persistence").decode('utf-8')
        if "loading:0" in data:
            break
        data =  execute(-1,redis_client,"info Persistence").decode('utf-8')

    data = execute(-1,redis_client,"MODULE LIST")
    print(data)
    graphdb_loaded = False
    indexer_loaded = False
    query_loaded = False
    rules_loaded = False
    for m in data:
        print(m)
        if m[b'name'].decode('utf-8') == "graphdb": graphdb_loaded= True
        if m[b'name'].decode('utf-8') == "rxIndexer": indexer_loaded= True
        if m[b'name'].decode('utf-8') == "RXQUERY": query_loaded= True
        if m[b'name'].decode('utf-8') == "rxRule": rules_loaded= True
    if not indexer_loaded:
        execute(-2,redis_client,"MODULE LOAD {}/rxIndexer.so INDEX 192.168.1.180 6401 0 DATA 192.168.1.180 6400 0".format(modulePath))
    if not query_loaded:
        data = execute(-2,redis_client,"MODULE LOAD {}/rxQuery.so   INDEX 192.168.1.180 6401 0 DATA 192.168.1.180 6400 0 DEFAULT_OPERATOR &".format(modulePath))
    if not rules_loaded:
        data = execute(-2,redis_client,"MODULE LOAD {}/rxRule.so   INDEX 192.168.1.180 6401 0 DATA 192.168.1.180 6400 0 ".format(modulePath))
    if not graphdb_loaded:
        execute(-2,redis_client,"MODULE LOAD {}/rxGraphdb.so    INDEX 192.168.1.180 6401 0 DATA 192.168.1.180 6400 0 ".format(modulePath))

    redis_index = redis.StrictRedis('192.168.1.180', 6401, 0)
    data = redis_index.execute_command("MODULE LIST")
    fetcher_loaded = False
    print(data)
    for m in data:
        print(m)
        if m[b'name'].decode('utf-8') == "rxIndexStore": fetcher_loaded= True
    if not fetcher_loaded:
        redis_index.execute_command("MODULE LOAD {}/rxIndexStore.so a b c".format(modulePath))

    # exit(0)    
    
    if must_flush:
        execute(-2,redis_client,"FLUSHALL")
        redis_index.execute_command("FLUSHALL")


    return (redis_client, redis_index)

def execute(n,client, cmd):
    print("{}: {}".format(n,cmd))
    r = client.execute_command(cmd)
    print("{}= {}".format(n,r))
    return r

def main(must_flush = False):
    run_length = 1
    repeat_length = 1
    query_repeat_length = 1
    t = check_server(must_flush)
    redis_client = t[0]
    redis_index = t[1]
    nfails = 0
    # exit(0)
    try:
        for n in range(0,run_length):
            print("TEST SIMPLE STRING")
            execute(n,redis_client,"flushall")
            redis_index.execute_command("flushall")
            for j in range(0,repeat_length):
                redis_client.delete("UN")
                redis_client.set("UN","United Nations")
                execute(n,redis_client,"rxindex wait")
                redis_index.keys("*")
                for j in range(0,query_repeat_length):
                    execute(n,redis_client,"RXQUERY united nations")
            execute(n,redis_client,"GET UN")
            execute(n,redis_client,"RXQUERY united | nations")
            execute(n,redis_client,"RXQUERY united & nations")

            print("TEST TEXT STRING")
            execute(n,redis_client,"flushall")
            redis_index.execute_command("flushall")
            for j in range(0,repeat_length):
                redis_client.delete("UN")
                redis_client.set("UN", "United Nations. HQ: New York. Established: 1945.")
                execute(n,redis_client,"rxindex wait")
                redis_index.keys("*")
                for j in range(0,query_repeat_length):
                    execute(n,redis_client,"RXQUERY united nations")
            execute(n,redis_client,"rxindex wait")
            execute(n,redis_client,"GET UN")
            execute(n,redis_client,"RXQUERY united & nations")
            execute(n,redis_client,"RXQUERY new | york")
            execute(n,redis_client,"RXQUERY new & york")
            execute(n,redis_client,"RXQUERY united | nations | 1945")
            execute(n,redis_client,"RXQUERY united & nations & 1945")

            print("TEST JSON STRING")
            execute(n,redis_client,"flushall")
            redis_index.execute_command("flushall")
            for j in range(0,repeat_length):
                redis_client.delete("UN")
                json ="{\"Organisation\": \"United Nations\", \"HQ\": \"New York\", \"Established\": \"1945\"}"
                print(json)
                redis_client.set("UN", json)
                execute(n,redis_client,"rxindex wait")
                redis_index.keys("*")
                for j in range(0,query_repeat_length):
                    execute(n,redis_client,"RXQUERY united nations")
            execute(n,redis_client,"GET UN")
            execute(n,redis_client,"RXQUERY united | nations")
            execute(n,redis_client,"RXQUERY united & nations")
            execute(n,redis_client,"RXQUERY new | york")
            execute(n,redis_client,"RXQUERY new & york")
            execute(n,redis_client,"RXQUERY united | nations | 1945")
            execute(n,redis_client,"RXQUERY united & nations & 1945")

            print("TEST SIMPLE HASH")
            execute(n,redis_client,"flushall")
            redis_index.execute_command("flushall")
            avp = {"name":"United Nations", "HQ":"New York", "Established":1945}
            for j in range(0,repeat_length):
                redis_client.delete("UN")
                redis_client.hmset('UN', avp)
                execute(n,redis_client,"rxindex wait")
                redis_index.keys("*")
                for j in range(0,query_repeat_length):
                    execute(n,redis_client,"RXQUERY united nations")
            execute(n,redis_client,"HGETALL UN")
            execute(n,redis_client,"rxindex wait")
            execute(n,redis_client,"RXQUERY united | nations")
            execute(n,redis_client,"RXQUERY united & nations")
            execute(n,redis_client,"RXQUERY new | york")
            execute(n,redis_client,"RXQUERY new & york")
            execute(n,redis_client,"RXQUERY united | nations | 1945")

            execute(n,redis_client,"RXQUERY united & nations & 1945")


    except  Exception as e:
        track = traceback.format_exc()
        print(track)
        print("---------------------")
        nfails += 1
        print("{} {}".format(nfails, e))
        if nfails >= 10: 
            return
        pass
 
    redis_client.close()
    redis_index.close()



if __name__ == "__main__":
    has_exception = False
    nfails = 0
    main(must_flush=True)
