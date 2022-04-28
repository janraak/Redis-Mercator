import redis
import pdb

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
    if not graphdb_loaded:
        redis_client.execute_command("MODULE LOAD src/modules/graphdb.so ")
    if not indexer_loaded:
        redis_client.execute_command("MODULE LOAD src/modules/rxIndexer.so 192.168.1.182 6379")
    if not query_loaded:
        data = redis_client.execute_command("MODULE LOAD src/modules/rxQuery.so  192.168.1.182 6379 \"&\"")
    if not rules_loaded:
        data = redis_client.execute_command("MODULE LOAD src/modules/rxRule.so")

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
    return redis_client

def main(must_flush = False):
    print("setting up")
    redis_client = check_server(must_flush)
    print("running test")
    cmd = "rule.set rule1 \"inout(\'Bible\', bible)\""
    print(cmd)
    data = redis_client.execute_command(cmd)
    print(data)
    cmd = "rule.set rule2 \"inout(\'chapter\',\'Chapter\')\""
    print(cmd)
    data = redis_client.execute_command(cmd)
    print(data)
    cmd = "rule.apply \"bible/en/nwtsty/Psalm/115/2611/v19-115-5-2\""
    print(cmd)
    data = redis_client.execute_command(cmd)
    print(data)

    redis_client.close()
if __name__ == "__main__":
    # has_exception = False
    # nfails = 0
    # while has_exception == False:
    #     try:
    #         # for n in range(0,10000):
    #         main(must_flush=False)
    #         exit
    #     except:
    #         nfails += 1
    #         print("failed ({}) to connect, will retry".format(nfails))
    #         if nfails < 100:
    #             has_exception = True
    #         else:
    #             pdb.set_trace()
    #             exit
    main(must_flush=False)

