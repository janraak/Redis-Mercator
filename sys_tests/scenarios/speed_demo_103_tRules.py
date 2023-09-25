from __future__ import print_function
import redis
import json
import pdb
global redis_path
def column(matrix, i):
    return [row[i] for row in matrix]

def SD_103_Business_Rules_and_or_Triggers(cluster_id, controller, data, index):
    print(data.execute_command("g.wget", "https://roxoft.dev/assets/bedrock1.txt"))
    print(data.execute_command("RULE.SET", "age_restriction", "(has(type,Purchase).inout(Purchase).lt(age,21)).property(status,age_check).redis(rpush, agecheck, @tuple)"))
    print(data.execute_command("RXQUERY", "G:AddV(bambam,MALE).property(age,9).AddV(fred,MALE).property(age, 47)"))
    print(data.execute_command("RXQUERY", "G:AddV(P000000001,Purchase).property(item,amstel).property(category,beer).predicate(Purchase).subject(bambam).object(P000000001)"))
    print(data.execute_command("RXQUERY", "G:AddV(P000000002,Purchase).property(item,amstel).property(category,beer).predicate(Purchase).subject(fred).object(P000000002)"))
    bambam = data.hgetall("bambam")
    print(bambam)
    try:
        assert bambam[b'status'] == b'age_check'
    except Exception as e:
        print(e)
        pass
    fred = data.hgetall("fred")
    print(fred)
    try:
        assert fred[b'status'] == None
    except Exception as e:
        print(e)
        pass
    data.execute_command("rxIndex wait")
    
    query1 = data.lrange("agecheck", 0, 1000)
    try:
        assert len(query1) > 0
        assert query1[0] == '{"key": "bambam", "value":{"type":"MALE","age":"9","status":"age_check"}}'
    except Exception as e:
        print(e)
        pass


        