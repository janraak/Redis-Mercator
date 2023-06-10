from __future__ import print_function
import redis
import json
import pdb

def scenario2_rule_integrity_check(cluster_id, controller, data, index):
    print(data.execute_command("info", "modules"))
    data.execute_command("rule.del", "*")
    data.execute_command("RULE.SET", "isAgeCheckRequired",
                         "(has(buys,alcohol).lt(age,21)).redis(rpush, agecheck, @tuple)")
    data.execute_command("hmset", "fred", "age", 39, "buys", "alcohol")
    data.execute_command("hmset", "bambam", "age", 5, "buys", "alcohol")
    for n in range(1, 1000):
        from_q = data.execute_command("lrange", "agecheck", 0, 10)
        if len(from_q) > 0:
            break
    # if len(from_q) <= 0:
    #     pdb.set_trace()
    print(from_q)
    assert len(from_q) > 0
    assert from_q[0].decode('utf-8').find("bambam") >= 0

