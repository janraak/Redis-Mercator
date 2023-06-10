from __future__ import print_function
import redis
import json


def scenario2_rule_age_check_add_property(cluster_id, controller, data, index):
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
    print(pebbles)
    assert pebbles[b'isMinor'] == b'Y'

