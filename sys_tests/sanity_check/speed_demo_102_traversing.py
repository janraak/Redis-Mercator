from __future__ import print_function
import redis
import json
import pdb
global redis_path
def column(matrix, i):
    return [row[i] for row in matrix]

def SD_102_Traversing_a_Redis_Database_Redis_as_a_GraphDB(cluster_id, controller, data, index):
    print(data.execute_command("g.wget", "https://roxoft.dev/assets/bedrock1.txt"))
    print(data.execute_command("g.set", "FILE", "bedrock1.txt"))
    fred = data.hgetall("fred")
    print(fred)
    try:
        assert fred[b'name'] == b'Fred Flintstone'
        assert fred[b'type'] == b'male'
    except Exception as e:
        print(e)
        pass
    data.execute_command("rxIndex wait")
    
    query1 = data.execute_command("rxquery", "g:v(male)", "ranked")
    query1_keys = column(query1,1)
    try:
        assert len(query1_keys) == 5
        assert b'bambam' in query1_keys
        assert b'barney' in query1_keys
        assert b'chip' in query1_keys
        assert b'fred' in query1_keys
        assert b'mrslate' in query1_keys
    except Exception as e:
        print(e)
        pass

    
    query1 = data.execute_command("rxquery", "g:v().out(owner)", "ranked")
    query1_keys = column(query1,1)
    try:
        assert len(query1_keys) == 1
        assert b'dino' in query1_keys
    except Exception as e:
        print(e)
        pass

    query2 = data.execute_command("rxquery", "g:v().match(fred,flo)", "ranked")
    try:
        assert len(query2) == 1
        link = query2[0]
        subject = column(link,1)
        length = column(link,3)
        object = column(link,5)
        assert b'fred' == subject
        assert b'flo' == object
        assert b'2' == length
    except Exception as e:
        print(e)
        pass
    
    query3 = data.execute_command("rxquery", "g:addv(hidea,female).property(name,'Hidea Frankenstone').property(image,'hidea.jpg').addv(frank,male).property(name,'Frank Frankenstone').property(image,'frank.jpg').addv(freaky,male).property(name,'Freaky Frankenstone').property(image,'freaky.jpg').addv(atrocia,female).property(name,'Atrocia Frankenstone').property(image,'atrocia.jpg')")
    query3 = data.execute_command("rxquery", "g:predicate(father,son).subject(frank).object(freaky).predicate(mother,son).subject(frank).object(freaky).predicate(father,daughter).subject(frank).object(atrocia).predicate(mother,daughter).subject(hidea).object(atrocia)")


    query2 = data.execute_command("rxquery", "g:v().match(freaky,atrocia)", "ranked")
    try:
        assert len(query2) == 1
        link = query2[0]
        subject = column(link,1)
        length = column(link,3)
        object = column(link,5)
        assert b'freaky' == subject
        assert b'atrocia' == object
        assert b'2' == length
    except Exception as e:
        print(e)
        pass

        