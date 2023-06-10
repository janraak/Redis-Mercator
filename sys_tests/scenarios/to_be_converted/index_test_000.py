import redis
import pdb
import traceback
import index_test
import index_test_persons
import index_test_persons2
import index_test_ev
import time
import sys

from os.path import abspath
import os.path
from pathlib import Path

def flushall(redis_client):
    redis_client.execute_command("FLUSHALL")
    redis_index = redis.StrictRedis('127.0.0.1', 6379, 0)
    redis_index.execute_command("FLUSHALL")
    redis_index.close()

def check_server(must_flush):

    print ('Number of arguments: {} arguments.'.format(len(sys.argv))) 
    print ('Argument List: {}'.format( str(sys.argv)))
    filename = abspath(sys.argv[0])
    print(filename)
    path = Path(sys.argv[0])
    modulePath = path.parent.parent.joinpath("src").absolute()
    print(modulePath)

    redis_client = redis.StrictRedis('127.0.0.1', 6379, 0)

    while True:
        data =  redis_client.execute_command("info Persistence").decode('utf-8')
        if "loading:0" in data:
            break
        data =  redis_client.execute_command("info Persistence").decode('utf-8')

    data = redis_client.execute_command("MODULE LIST")
    print(data)
    graphdb_loaded = False
    indexer_loaded = False
    query_loaded = False
    rules_loaded = False
    mercator_loaded = False
    for m in data:
        print(m)
        if m[b'name'].decode('utf-8') == "graphdb": graphdb_loaded= True
        if m[b'name'].decode('utf-8') == "rxIndexer": indexer_loaded= True
        if m[b'name'].decode('utf-8') == "RXQUERY": query_loaded= True
        if m[b'name'].decode('utf-8') == "rxRule": rules_loaded= True
        if m[b'name'].decode('utf-8') == "rxMercator": mercator_loaded= True
    if not mercator_loaded:
        redis_client.execute_command("MODULE LOAD {}/rxMercator.so RB_A 127.0.0.1 8MB 500  RB_A 127.0.0.1 8MB 500  RB_A 127.0.0.1 8MB 500 ".format(modulePath))
    if not indexer_loaded:
        redis_client.execute_command("MODULE LOAD {}/rxIndexer.so 127.0.0.1 6379".format(modulePath))
    if not query_loaded:
        data = redis_client.execute_command("MODULE LOAD {}/rxQuery.so  127.0.0.1 6379 &".format(modulePath))
    if not rules_loaded:
        data = redis_client.execute_command("MODULE LOAD {}/rxRule.so".format(modulePath))
    if not graphdb_loaded:
        redis_client.execute_command("MODULE LOAD {}/rxGraphdb.so ".format(modulePath))

    redis_index = redis.StrictRedis('127.0.0.1', 6379, 0)
    data = redis_index.execute_command("MODULE LIST")
    fetcher_loaded = False
    print(data)
    for m in data:
        print(m)
        if m[b'name'].decode('utf-8') == "rxIndexStore": fetcher_loaded= True
    if not fetcher_loaded:
        redis_index.execute_command("MODULE LOAD /home/pi/redis/redis-6.0.10/extensions/src/rxIndexStore.so a b c")

    # exit(0)    
    
    if must_flush:
        flushall(redis_client)

    redis_index.close()

    return redis_client

def load_and_index(redis_client, persons):
    # pdb.set_trace()
    for uri in persons:
        redis_client.execute_command("SET",uri, persons[uri])
    redis_client.execute_command("rxIndex WAIT")

def get_command_from_test(test):
    if "q" in test:
        q = "rxquery"
        for t in test["q"]:
            q += " " + t
        return q
    if "f" in test:
        return "rxquery {} == {}".format(test["f"],test["v"])
    else:
        return "rxquery {}".format(test["v"])
    
def verify_load_and_index(redis_client, expect):
    errors = 0
    redis_client.execute_command("rxIndex WAIT")
    # pdb.set_trace()
    for test in expect:
        expectation = test["k"]
        expectation_size = len(expectation)
        command = get_command_from_test(test)
        keys = redis_client.execute_command(command)
        if "p" in test:
            print("\n\n{}\n\n{}\n\n".format(command, keys))
        if len(keys) != expectation_size:
            keys = redis_client.execute_command(command)
            if len(keys) != expectation_size:
                print("{}\nexpected   {}\nkeys found {} {}".format(test, expectation_size, len(keys), keys.sort()))
                errors += 1
            if type(keys) is list:
                if keys.sort != expectation.sort():
                    print("{}\nexpected   {}\nkeys found {}".format(test, expectation.sort(), keys.sort()))
                    errors += 1
    return errors

def main(must_flush = False):
    redis_client = check_server(must_flush)
    load_and_index(redis_client,index_test.book)
    load_and_index(redis_client,index_test_persons.persons)
    print("Simple string test, {} errors".format(verify_load_and_index(redis_client,index_test.expect)))
    print("Json string test, {} errors".format(verify_load_and_index(redis_client,index_test_persons.expect)))
    flushall(redis_client)
    load_and_index(redis_client,index_test_persons2.persons)
    time.sleep(3)
    print("Simple structured string test, {} errors".format(verify_load_and_index(redis_client,index_test_persons2.expect)))
    flushall(redis_client)
    load_and_index(redis_client,index_test_ev.cars)
    time.sleep(3)
    print("EV Simple structured string test, {} errors".format(verify_load_and_index(redis_client,index_test_ev.expect)))
    redis_client.close()



if __name__ == "__main__":
    has_exception = False
    nfails = 0
    main(must_flush=True)
