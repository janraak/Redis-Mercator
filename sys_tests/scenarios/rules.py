from __future__ import print_function
import redis
import json


def scenario2_rule_integrity_check(cluster_id, controller, data, index):
    controller.execute_command("mercator.flush.cluster {}".format(cluster_id))
    data.execute_command("rule.del *")
    data.execute_command("RULE.SET", "isAgeCheckRequired",
                         "(has(buys,alcohol).lt(age,21)).redis(rpush, agecheck, @tuple)")
    data.execute_command("hmset", "fred", "age", 39, "buys", "alcohol")
    data.execute_command("hmset", "bambam", "age", 5, "buys", "alcohol")
    for n in range(1, 1000):
        from_q = data.execute_command("lrange", "agecheck", 0, 10)
        if len(from_q) > 0:
            break
    if len(from_q) <= 0:
        pdb.set_trace()
    assert len(from_q) > 0
    assert from_q[0].decode('utf-8').find("bambam") >= 0


def scenario2_rule_age_check_add_property(cluster_id, controller, data, index):
    controller.execute_command("mercator.flush.cluster {}".format(cluster_id))
    data.execute_command("rule.del *")
    data.execute_command("hmset", "pebbles", "type",
                         "person", "gender", "female")
    data.execute_command("RULE.SET", "isMinor",
                         "(has(type,person).lt(age,21)).property(isMinor,Y)")
    pebbles = data.hgetall("pebbles")
    assert not (b'isMinor' in pebbles)
    data.execute_command("hmset", "pebbles", "age", 5)
    for n in range(1, 1000):
        pebbles = data.hgetall("pebbles")
        if (b'isMinor' in pebbles):
            break
    assert pebbles[b'isMinor'] == b'Y'


def scenario2_rule_create_edges(cluster_id, controller, data, index):
    controller.execute_command("mercator.flush.cluster {}".format(cluster_id))
    data.execute_command("rule.del", "*")

    data.execute_command("RULE.SET", "linkFather",
                         "haslabel(father).addE(@,'father','father_of')")
    data.execute_command("RULE.SET", "linkMother",
                         "haslabel(mother).addE(@,'mother','mother_of')")
    data.execute_command("RULE.SET", "linkSpouse",
                         "haslabel(spouse).addE(@,'spouse','spouse_of')")
    rules = data.execute_command("rule.list")
    print(rules)

    data.execute_command("hmset", "fred", "gender", "male", "type", "person")
    data.execute_command("hmset", "wilma", "gender",
                         "female", "type", "person")
    data.execute_command("hmset", "pebbles", "type", "person")
    data.execute_command("hmset", "barney", "gender", "male", "type", "person")
    data.execute_command("hmset", "betty", "gender",
                         "female", "type", "person")
    data.execute_command("hmset", "bambam", "type", "person")

    pebbles = data.hgetall("pebbles")
    assert not (b'father' in pebbles)
    assert not (b'mother' in pebbles)

    bambam = data.hgetall("bambam")
    assert not (b'father' in bambam)
    assert not (b'mother' in bambam)

    fred = data.hgetall("fred")
    assert not (b'spouse' in fred)
    assert not (b'spouse_of' in fred)

    wilma = data.hgetall("wilma")
    assert not (b'spouse' in wilma)
    assert not (b'spouse_of' in wilma)

    data.execute_command("hmset", "fred", "spouse", "wilma")
    data.execute_command("hmset", "wilma", "spouse", "fred")
    data.execute_command("hmset", "pebbles", "father",
                         "fred", "mother", "wilma")
    data.execute_command("hmset", "barney", "spouse", "betty")
    data.execute_command("hmset", "betty", "spouse", "barney")
    data.execute_command("hmset", "bambam", "father",
                         "barney", "mother", "betty")

    for n in range(1, 1000):
        bambam = data.hgetall("bambam")
        if (b'mother' in bambam):
            break

    keys_to_check = ["^spouse_of:fred:wilma",
                     "^father:pebbles:fred",
                     "^mother_of:wilma:pebbles",
                     "^mother_of:betty:bambam",
                     "^father_of:fred:pebbles",
                     "^mother:pebbles:wilma",
                     "^spouse:wilma:fred",
                     "^spouse_of:wilma:fred",
                     ]
    rules = data.execute_command("rule.list")
    print(rules)

    for key in keys_to_check:
        for n in range(1,1000):
            smembers = data.smembers(key)
            if len(smembers) > 0:
                break
        if len(smembers) <= 0:
            print("{} --> {}".format(key, smembers))
        assert len(smembers) > 0

    fred = data.execute_command("g.get", "fred", "2")
    print(fred)

    bambam = data.execute_command("g.get", "bambam", "2")
    print(bambam)
