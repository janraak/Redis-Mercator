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

    redis_client = redis.StrictRedis('192.168.1.180', 6379, 0)

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
    for m in data:
        print(m)
        if m[b'name'].decode('utf-8') == "graphdb": graphdb_loaded= True
        if m[b'name'].decode('utf-8') == "rxIndexer": indexer_loaded= True
        if m[b'name'].decode('utf-8') == "RXQUERY": query_loaded= True
        if m[b'name'].decode('utf-8') == "rxRule": rules_loaded= True
    if not indexer_loaded:
        redis_client.execute_command("MODULE LOAD {}/rxIndexer.so 192.168.1.182 6379".format(modulePath))
    if not query_loaded:
        data = redis_client.execute_command("MODULE LOAD {}/rxQuery.so  192.168.1.182 6379 &".format(modulePath))
    if not rules_loaded:
        data = redis_client.execute_command("MODULE LOAD {}/rxRule.so".format(modulePath))
    if not graphdb_loaded:
        redis_client.execute_command("MODULE LOAD {}/rxGraphdb.so ".format(modulePath))

    redis_index = redis.StrictRedis('192.168.1.182', 6379, 0)
    data = redis_index.execute_command("MODULE LIST")
    fetcher_loaded = False
    print(data)
    for m in data:
        print(m)
        if m[b'name'].decode('utf-8') == "rxFetch": fetcher_loaded= True
    if not fetcher_loaded:
        redis_index.execute_command("MODULE LOAD /home/pi/redis/redis-6.0.10/extensions/src/rxFetch.so a b c")
    
    if must_flush:
        redis_client.execute_command("FLUSHALL")
        redis_index.execute_command("FLUSHALL")

    redis_client.execute_command("RULE.SET $$ISPERSON has(type,person)")
    redis_client.execute_command("RULE.SET $$ISCOUNTRY has(type,country)")
    redis_client.execute_command("RULE.SET $$ISOUDERVAN hasout(zoon,dochter).out(zoon,dochter).by(subject)")
    redis_client.execute_command("RULE.SET $$ISOUDERVAN2 ((as(all).hasout(dochter)|as(all).hasout(zoon)))")
    redis_client.execute_command("RULE.SET $$ISKINDVAN hasout(zoon,dochter).out(zoon,dochter).by(object)")
    redis_client.execute_command("RULE.SET $$RAAK has(family,raak).property(target,yes)")
    redis_client.execute_command("RULE.SET $$HORST has(family,horst).property(target,yes)")
    redis_client.execute_command("RULE.SET $$OUDERSBEKEND (hasin(vader)|hasin(moeder)).property(known_parent,yes)")
    redis_index.close()

    return redis_client

def rxfetch_test(redis_client):
    # redis_client.execute_command("rxadd NL H kleur rood 0.333")
    # redis_client.execute_command("rxadd NL H kleur wit 0.333")
    # redis_client.execute_command("rxadd NL H kleur blauw 0.333")
    # redis_client.execute_command("rxadd NL H kleur blaur 0.333")
    # redis_client.execute_command("rxbegin NL")
    # redis_client.execute_command("rxadd NL H kleur rood 0.333")
    # redis_client.execute_command("rxadd NL H kleur wit 0.333")
    # redis_client.execute_command("rxadd NL H kleur blauw 0.333")
    # redis_client.execute_command("rxcommit NL")
    pass

def verify_vertice(redis_client, key, must_have_members):
    # print("#vv1 {}".format(key))
    h = redis_client.hgetall(key)
    # print("#vv2 {} ==? {}".format(h[b'type'], b'person'))
    # assert h[b'type'] == b'person'

    try:
        m = redis_client.smembers('^'+key)
        assert (len(m) == 0) == must_have_members
    except ConnectionError:
        print("ConnectionError on smember {}".format('^'+key))
    except:
        print("Error on smember {}".format('^'+key))

def verify_edge(redis_client,si,predicates,oi):
    # print("#ev1 {} {} {}".format(si,predicates,oi))

    s_to_o = "{}:{}:{}".format(predicates[0],si,oi)
    if predicates[0] != predicates[1]:
        o_to_s = "{}:{}:{}".format(predicates[1],oi,si)
    else:
        o_to_s = s_to_o
    # pdb.set_trace()
    try:
        s_to_o_t = redis_client.hget(s_to_o, "type").decode()
        o_to_s_t = redis_client.hget(o_to_s, "type").decode()
    # 
        # print("#ev2 {} {} {} {}->{} {}->{}".format(si,predicates,oi, s_to_o, s_to_o_t, s_to_o, o_to_s_t))

        assert s_to_o_t == predicates[0]
        assert o_to_s_t == predicates[1]
    except ConnectionError:
        print("ConnectionError on  {}x{}".format(s_to_o, o_to_s))
    except:
        print("Error on  {}x{}".format(s_to_o, o_to_s))

def verify_query(client, question, query, description, cardinality, feature = None, aspect= None):
    # print(question)
    # print(query)
    # # pdb.set_trace()
    # print(description)
    try:
        data = client.execute_command(query)
        # data = data.replace("\r", "\n")
        if len(data) != cardinality:
            print(">>>>>>>>>>>>>>>>>>>>>>>>>>> test failed")
            print(question)
            print(query)
            # pdb.set_trace()
            print(description)
            print(data)
            print(">>>>>>>>>>>>>>>>>>>>>>>>>>> test failed\nquery returned {} rows\n               {} expected".format(len(data), cardinality))
            assert len(data) == cardinality
    except ConnectionError:
        print("ConnectionError on  {}".format(query))
        print(question)
        print(query)
        # pdb.set_trace()
        print(description)
    except:
        print("Error on {}".format(query))
        print(question)
        print(query)
        # pdb.set_trace()
        print(description)

def main(must_flush = False):
    run_length = 500
    redis_client = check_server(must_flush)
    nfails = 0
    try:
        rxfetch_test(redis_client)

        for t in dataset_family.get_dataset():
            si = ""
            oi = ""
            for j in ["s", "o"]:
                person = t[j]
                props = ""
                et = "person"
                # pdb.set_trace()
                if type(person) == dict:
                    iri = person["k"]
                    # print("#2{}: {} {}".format(iri, type(person),person))
                    for (a,v) in person.items():
                        # print("#2 {} = {}".format(a,v))
                        if a == "k":
                            continue
                        if a == "t":
                            et = v
                            continue
                        props = "{}.property(\'{}\',\'{}\')".format(props,a,v)
                else:
                    iri = person
                if j == "s": 
                    si = iri
                else:
                    oi = iri
                # print("#1 {}: {} {}".format(type(person),person,props))
                if not redis_client.exists(iri):
                    cmd = "rxquery " + "  reset   g:addV(\'{}\',\'{}\'){}".format(iri, et, props)
                    print(cmd)
                    data = redis_client.execute_command(cmd)
                    # print("{}: {}".format(iri,data))
                    verify_vertice(redis_client, iri, True)
            eprops = ""
            if type(t["p"]) == dict:
                predicate = t["p"]
                predicates = predicate["r"]
                for (a,v) in predicate.items():
                    # print("e#2 {} = {}".format(a,v))
                    if a == "r":
                        continue
                    eprops = "{}.property(\'{}\',\'{}\')".format(eprops,a,v)
            else:
                predicates = t["p"]
            # pdb.set_trace()
            if type(predicates) != list:
                predicates = [predicates, predicates]
            cmd = "rxquery " + "  reset g.addE(\'{}\',\'{}\',\'{}\',\'{}\'){}".format(si, predicates[0], predicates[1], oi, eprops)
            print(cmd)

            data = redis_client.execute_command(cmd)
            print(data)
            verify_vertice(redis_client, si, False)
            verify_vertice(redis_client, oi, False)
            verify_edge(redis_client,si,predicates,oi)
        # return
        time.sleep(10)

        # for n in range(0,-1):
        #     verify_query(redis_client, \
        #         "# Test C++ extensions", \
        #         "TestG", \
        #         "C++ extensions should work", \
        #         0,
        #         feature=["c++", "gremlin"],
        #         aspect=[])
        #     verify_query(redis_client, \
        #         "# Test C++ extensions", \
        #         "TestG", \
        #         "C++ extensions should work", \
        #         0,
        #         feature=["c++", "gremlin"],
        #         aspect=[])

        # return
        cmds_to_test = ["rxQuery"]
        for cmd_to_test in cmds_to_test:
            print(cmds_to_test)
            for n in range(0,run_length):
                verify_query(redis_client, \
                    "# Who are a persons parent?", \
                    cmd_to_test + "  reset  g.v.(as(all).in(vader)|as(all).in(moeder)).by(subject)", \
                    "All persons with it's known father and/or mother", \
                    14,
                    feature=['match'],
                    aspect=['path'])
                    
                verify_query(redis_client, \
                    "# Who are a persons children?", \
                    cmd_to_test + "  reset  g.v.(as(all).in(vader)|as(all).in(moeder)).by(object)", \
                    "All persons with it's known children", \
                    14,
                    feature=['match'],
                    aspect=['path'])

                verify_query(redis_client, \
                    "# Who are a persons parent?", \
                    cmd_to_test + "  reset  g.v.in(vader,moeder)).by(subject)", \
                    "All persons with it's known father and/or mother", \
                    14,
                    feature=['match'],
                    aspect=['path'])
                    
                verify_query(redis_client, \
                    "# Who are a persons children?", \
                    cmd_to_test + "  reset  g.v.in(vader,moeder).by(object)", \
                    "All persons with it's known children", \
                    14,
                    feature=['match'],
                    aspect=['path'])

                # Who are sibblings?
                # person-1 -> in(vader,moeder) 
                # person-2 -> in(vader,moeder)
                # 
                # pdb.set_trace()
                verify_query(redis_client, \
                    "# Who lives or lived in the NL?", \
                    cmd_to_test + "  g.exclude(zoon,dochter,vader,moeder,echtgenoot,echtgenote,woonplaats_van).match(as($$ISPERSON),as($$ISCOUNTRY).has(abbr,NL))", \
                    "All persons with living/lived in the NL", \
                    4,
                    feature=['match', 'exclude'],
                    aspect=['path'])


                verify_query(redis_client, \
                    "# Who are one's parents", \
                    cmd_to_test + "  g.v().as(all).out(zoon)|as(all).out(dochter)).by(object) ", \
                    "All persons with his/her parents", \
                    14,
                    feature=['OR', 'as', 'groupBy'],
                    aspect=['triplet'])


                verify_query(redis_client, \
                    "# Who are one's children", \
                    cmd_to_test + "  g.v().as(all).out(zoon)|as(all).out(dochter)).by(subject) ", \
                    "All persons with their children", \
                    14,
                    feature=['OR', 'as', 'groupBy'],
                    aspect=['triplet'])


                verify_query(redis_client, \
                    "# Who's father is unknown", \
                    cmd_to_test + "  reset  g.v.(as(all).has(type,person) ! as(all).hasin(vader))", \
                    "All persons with an unknown father", \
                    4,
                    feature=['nomatch', 'as', 'not in'],
                    aspect=['triplet'])

                verify_query(redis_client, \
                    "# Who's mother is unknown", \
                    cmd_to_test + "  reset  g.v.(as(all).has(type,person) ! as(all).hasin(moeder))", \
                    "All persons with an unknown mother", \
                    8,
                    feature=['nomatch', 'as', 'not in'],
                    aspect=['triplet'])

                verify_query(redis_client, \
                    "# Who has a son or a daughter", \
                    cmd_to_test + "  reset  g.v().out(zoon,dochter).by(subject)", \
                    "All persons with a son or a daughter", \
                    14,
                    feature=['out','groupby'],
                    aspect=[])

                verify_query(redis_client, \
                    "# Who has a trigger updated property", \
                    cmd_to_test + "  reset  g.v().has(target,yes)", \
                    "All persons trigger updated", \
                    9,
                    feature=['has'],
                    aspect=[])

                verify_query(redis_client, \
                    "# Who has a trigger updated property", \
                    cmd_to_test + "  reset  g.v().hasLabel(target)", \
                    "All target trigger updated", \
                    9,
                    feature=['hasLabel'],
                    aspect=[])

                # pdb.set_trace()
                verify_query(redis_client, \
                    "# All persons", \
                    cmd_to_test + "  reset  g.v().has(type,person)", \
                    "All persons", \
                    22,
                    feature=['has'],
                    aspect=[])

                verify_query(redis_client, \
                    "# Who has not been a trigger updated", \
                    cmd_to_test + "  reset  g.v().missingLabel(target).has(type,person)", \
                    "All persons NOT trigger updated", \
                    13,
                    feature=['missingLabel', 'has'],
                    aspect=[])


                cmd = cmd_to_test + "  reset  g.v().out(zoon,dochter)"
                # print("# Who all sibblings has a son or a daughter".format(cmd))
    except  Exception as e:
        track = traceback.format_exc()
        print(track)
        print("---------------------")
        nfails += 1
        print("{} {}".format(nfails, e))
        if nfails >= 10: 
            return
        pass
 
    try:
        cmds_to_test = ["rxquery"]
        for cmd_to_test in cmds_to_test:
            for n in range(0,run_length):
                verify_query(redis_client, \
                    "# $$ISPERSON", \
                    cmd_to_test + " g.has(type,person)", \
                    "$$ISPERSON", \
                    22,
                    feature=['match'],
                    aspect=['path'])
                    
                verify_query(redis_client, \
                    "# $$ISCOUNTRY", \
                    cmd_to_test + "  reset  g.has(type,country)", \
                    "$$ISCOUNTRY", \
                    2,
                    feature=['match'],
                    aspect=[])

                # pdb.set_trace()                
                verify_query(redis_client, \
                    "# $$ISOUDERVAN", \
                    cmd_to_test + "  reset  g:hasout(zoon,dochter).out(zoon,dochter).by(subject)", \
                    "$$ISOUDERVAN", \
                    14,
                    feature=['match', 'groupby'],
                    aspect=[])

                # pdb.set_trace()
                verify_query(redis_client, \
                    "# $$ISKINDVAN", \
                    cmd_to_test + "  g.hasout(zoon,dochter).out(zoon,dochter).by(object)", \
                    "$$ISKINDVAN", \
                    14,
                    feature=['match', 'exclude'],
                    aspect=['path'])

                verify_query(redis_client, \
                    "# $$RAAK", \
                    cmd_to_test + "  reset  g.has(family,raak).property(target,yes)", \
                    "$$RAAK", \
                    7,
                    feature=['nomatch', 'as', 'in'],
                    aspect=['triplet'])

                verify_query(redis_client, \
                    "# $$HORST", \
                    cmd_to_test + "  reset  g.has(family,horst).property(target,yes)", \
                    "$$HORST", \
                    2,
                    feature=['out','groupby'],
                    aspect=[])

                verify_query(redis_client, \
                    "# $$OUDERSBEKEND", \
                    cmd_to_test + "  reset  g.(as(x).hasin(vader)|as(x).hasin(moeder)).property(known_parent,yes)", \
                    "$$OUDERSBEKENDd", \
                    14,
                    feature=['has'],
                    aspect=[])
    except  Exception as e:
        track = traceback.format_exc()
        print(track)
        print("---------------------")
        nfails += 1
        print("{} {}".format(nfails, e))
        if nfails >= 10: 
            return
        pass
    data = redis_client.execute_command("RXCACHE")
    print("RXCACHE       {}".format(data.decode()))
    data = redis_client.execute_command("RXCACHE RESET")
    print("RXCACHE RESET {}".format(data.decode()))
    data = redis_client.execute_command("RULE.list")
    print("RULE.list\n {}".format(data.decode()))
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
    # while has_exception == False:
    #     try:
    #         # for n in range(0,10000):
    #         main(must_flush=True)
    #         exit
    #     except:
    #         nfails += 1
    #         print("failed ({}) to connect, will retry".format(nfails))
    #         if nfails < 100:
    #             has_exception = True
    #         else:
    #             pdb.set_trace()
    #             exit

