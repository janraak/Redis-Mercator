import redis
import pdb
import traceback

def check_server(must_flush):
    redis_client = redis.StrictRedis('192.168.1.180', 6379, 0)

    while True:
        data =  redis_client.execute_command("info Persistence").decode('utf-8')
        if "loading:0" in data:
            break
        data =  redis_client.execute_command("info Persistence").decode('utf-8')

    data = redis_client.execute_command("MODULE LIST")
    graphdb_loaded = False
    indexer_loaded = False
    query_loaded = False
    rules_loaded = False
    for m in data:
        if m[b'name'].decode('utf-8') == "graphdb": graphdb_loaded= True
        if m[b'name'].decode('utf-8') == "rxIndexer": indexer_loaded= True
        if m[b'name'].decode('utf-8') == "RXQUERY": query_loaded= True
        if m[b'name'].decode('utf-8') == "rxRule": rules_loaded= True
    if not indexer_loaded:
        redis_client.execute_command("MODULE LOAD src/modules/rxIndexer.so 192.168.1.182 6379")
    if not query_loaded:
        data = redis_client.execute_command("MODULE LOAD src/modules/rxQuery.so  192.168.1.182 6379 &")
    if not rules_loaded:
        data = redis_client.execute_command("MODULE LOAD src/modules/rxRule.so")
    if not graphdb_loaded:
        redis_client.execute_command("MODULE LOAD src/modules/graphdb.so ")

    redis_index = redis.StrictRedis('192.168.1.182', 6379, 0)
    data = redis_index.execute_command("MODULE LIST")
    fetcher_loaded = False
    for m in data:
        if m[b'name'].decode('utf-8') == "rx-fetch": fetcher_loaded= True
    if not fetcher_loaded:
        redis_index.execute_command("MODULE LOAD redis-query-rust/target/debug/examples/librx_fetch.so a b c")

    if must_flush:
        redis_client.execute_command("FLUSHALL")
        redis_index.execute_command("FLUSHALL")

    redis_client.execute_command("RULE.SET $$$RAAK has(family,raak).property(target,yes)")
    redis_client.execute_command("RULE.SET $$$HORST has(family,horst).property(target,yes)")
    redis_client.execute_command("RULE.SET $$$OUDERSBEKEND has(type,person).(hasin(vader)|hasin(moeder)).property(target,yes)")
    redis_client.execute_command("RULE.SET $$$ISOUDERVAN has(type,person).hasout(zoon,dochter).out(zoon,dochter).by(subject)")
    redis_client.execute_command("RULE.SET $$$ISKINDVAN has(type,person).hasout(zoon,dochter).out(zoon,dochter).by(object)")
    return redis_client

def main(must_flush = False):

    redis_client = check_server(must_flush)
    pdb.set_trace()
    print(redis_client.hset("jan", "doi", "1953-11-18"))
    print(redis_client.hset("jan", "family", "raak"))
    print(redis_client.hset("isabel", "doi", "1955-11-23"))
    print(redis_client.hset("isabel", "family", "horst"))
    print(redis_client.hgetall("jan"))
    print(redis_client.hgetall("isabel"))
    # redis_client.execute_command("MODULE UNLOAD rxRule")
    # redis_client.execute_command("MODULE UNLOAD graphdb")
    # redis_client.execute_command("MODULE UNLOAD rxIndexer")
    # redis_client.execute_command("MODULE UNLOAD RX_QUERY")
    # redis_client.execute_command("shutdown nosave") 
    redis_client.close()
if __name__ == "__main__":
    has_exception = False
    nfails = 0
    main(must_flush=True)

