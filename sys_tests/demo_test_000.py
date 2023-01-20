import redis
import pdb
import traceback
import time
import sys
import json
from os import system
from os.path import abspath
import os.path
from pathlib import Path
import subprocess

global redis_path
global dataset1
global dataset1ref


def scenario1_as_hash(cluster_id, controller, data, index, dataset1):
    controller.execute_command("mercator.flush.cluster {}".format(cluster_id))
    data.execute_command("load.text", "BAR",
                         "SEMICOLON_SEPARATED", dataset1, "as_hash")
    data.execute_command("rxIndex wait")
    RMA = data.hgetall("RMA")
    assert RMA[b'identity'] == b'Rijksmuseum'
    assert RMA[b'country'] == b'Netherlands'
    assert RMA[b'Year_Founded'] == b'1798'
    assert RMA[b'categories'] == b'museum,art,rembrandt,paintings'
    SBNNO = data.hgetall("SBNNO")
    assert SBNNO[b'identity'] == b'Steamboat Natchez'
    assert SBNNO[b'country'] == b'USA'
    assert SBNNO[b'categories'] == b'landmark,culture,transport,historic'
    assert SBNNO[b'Year_Founded'] == b'1823'
    HSPR = data.execute_command("rxquery russia 1764")
    HSPR = HSPR[0]  # Only first row
    assert HSPR[1+HSPR.index(b'key')] == b'HSPR'
    assert HSPR[1+HSPR.index(b'type')] == b'H'
    assert float(HSPR[1+HSPR.index(b'score')]) > 0
    HSPR = data.execute_command("rxget russia 1764")
    HSPR = HSPR[0]  # Only first row
    HSPR_VAL = HSPR[1+HSPR.index(b'value')]
    assert HSPR[1+HSPR.index(b'key')] == b'HSPR'
    assert float(HSPR[1+HSPR.index(b'score')]) > 0
    assert HSPR_VAL[1+HSPR_VAL.index(b'identity')] == b'Hermitage'
    assert HSPR_VAL[1+HSPR_VAL.index(b'country')] == b'Russia'
    assert HSPR_VAL[1+HSPR_VAL.index(b'categories')
                    ] == b'museum,art,paintings,sculptures'
    assert HSPR_VAL[1+HSPR_VAL.index(b'Year_Founded')] == b'1764'


def scenario1_as_string(cluster_id, controller, data, index, dataset1):
    controller.execute_command("mercator.flush.cluster {}".format(cluster_id))
    data.execute_command("load.text", "BAR",
                         "SEMICOLON_SEPARATED", dataset1, "as_string")
    RMA = data.execute_command("get", "RMA").decode('utf-8')
    assert RMA.find('Rijksmuseum') >= 0
    assert RMA.find('Netherlands') >= 0
    assert RMA.find('1798') >= 0
    assert RMA.find('museum,art,rembrandt,paintings') >= 0
    SBNNO = data.execute_command("get", "SBNNO").decode('utf-8')
    assert SBNNO.find('Steamboat Natchez') >= 0
    assert SBNNO.find('USA') >= 0
    assert SBNNO.find('landmark,culture,transport,historic') >= 0
    assert SBNNO.find('1823') >= 0
    data.execute_command("rxIndex wait")
    HSPR = data.execute_command("rxquery russia 1764")
    HSPR = HSPR[0]  # Only first row
    assert HSPR[1+HSPR.index(b'key')] == b'HSPR'
    assert HSPR[1+HSPR.index(b'type')] == b'S'
    assert float(HSPR[1+HSPR.index(b'score')]) > 0
    HSPR = data.execute_command("rxget russia 1764")
    HSPR = HSPR[0]  # Only first row
    HSPR_VAL = HSPR[1+HSPR.index(b'value')].decode('utf-8')
    assert HSPR[1+HSPR.index(b'key')] == b'HSPR'
    assert float(HSPR[1+HSPR.index(b'score')]) > 0
    assert HSPR_VAL.find('Hermitage') >= 0
    assert HSPR_VAL.find('Russia') >= 0
    assert HSPR_VAL.find('museum,art,paintings,sculptures') >= 0
    assert HSPR_VAL.find('1764') >= 0


def scenario1_as_text(cluster_id, controller, data, index, dataset1):
    controller.execute_command("mercator.flush.cluster {}".format(cluster_id))
    data.execute_command("load.text", "BAR",
                         "SEMICOLON_SEPARATED", dataset1, "as_text")
    RMA = data.execute_command("get", "RMA").decode('utf-8')
    assert RMA.find('identity:Rijksmuseum;') >= 0
    assert RMA.find('country:Netherlands;') >= 0
    assert RMA.find('Year_Founded:1798;') >= 0
    assert RMA.find('categories:museum,art,rembrandt,paintings;') >= 0
    SBNNO = data.execute_command("get", "SBNNO").decode('utf-8')
    assert SBNNO.find('identity:Steamboat Natchez;') >= 0
    assert SBNNO.find('country:USA;') >= 0
    assert SBNNO.find('categories:landmark,culture,transport,historic;') >= 0
    assert SBNNO.find('Year_Founded:1823;') >= 0
    data.execute_command("rxIndex wait")
    HSPR = data.execute_command("rxquery russia 1764")
    HSPR = HSPR[0]  # Only first row
    assert HSPR[1+HSPR.index(b'key')] == b'HSPR'
    assert HSPR[1+HSPR.index(b'type')] == b'S'
    assert float(HSPR[1+HSPR.index(b'score')]) > 0
    HSPR = data.execute_command("rxget russia 1764")
    HSPR = HSPR[0]  # Only first row
    HSPR_VAL = HSPR[1+HSPR.index(b'value')].decode('utf-8')
    assert HSPR[1+HSPR.index(b'key')] == b'HSPR'
    assert float(HSPR[1+HSPR.index(b'score')]) > 0
    assert HSPR_VAL.find('identity:Hermitage;') >= 0
    assert HSPR_VAL.find('country:Russia;') >= 0
    assert HSPR_VAL.find('categories:museum,art,paintings,sculptures;') >= 0
    assert HSPR_VAL.find('Year_Founded:1764;') >= 0


def scenario1_as_json(cluster_id, controller, data, index, dataset1):
    controller.execute_command("mercator.flush.cluster {}".format(cluster_id))
    data.execute_command("load.text", "BAR",
                         "SEMICOLON_SEPARATED", dataset1, "as_json")
    RMA = json.loads(data.execute_command("get", "RMA").decode('utf-8'))
    assert RMA['identity'] == 'Rijksmuseum'
    assert RMA['country'] == 'Netherlands'
    assert RMA['Year_Founded'] == '1798'
    assert RMA['categories'] == 'museum,art,rembrandt,paintings'
    SBNNO = json.loads(data.execute_command("get", "SBNNO").decode('utf-8'))
    assert SBNNO['identity'] == 'Steamboat Natchez'
    assert SBNNO['country'] == 'USA'
    assert SBNNO['categories'] == 'landmark,culture,transport,historic'
    assert SBNNO['Year_Founded'] == '1823'
    data.execute_command("rxIndex wait")
    HSPR = data.execute_command("rxquery russia 1764")
    HSPR = HSPR[0]  # Only first row
    assert HSPR[1+HSPR.index(b'key')] == b'HSPR'
    assert HSPR[1+HSPR.index(b'type')] == b'S'
    assert float(HSPR[1+HSPR.index(b'score')]) > 0
    HSPR = data.execute_command("rxget russia 1764")
    HSPR = HSPR[0]  # Only first row
    HSPR_VAL = json.loads(HSPR[1+HSPR.index(b'value')].decode('utf-8'))
    assert HSPR[1+HSPR.index(b'key')] == b'HSPR'
    assert float(HSPR[1+HSPR.index(b'score')]) > 0
    assert HSPR_VAL['identity'] == 'Hermitage'
    assert HSPR_VAL['country'] == 'Russia'
    assert HSPR_VAL['categories'] == 'museum,art,paintings,sculptures'
    assert HSPR_VAL['Year_Founded'] == '1764'


def scenario2_rule_integrity_check(cluster_id, controller, data, index, dataset1):
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


def scenario2_rule_age_check_add_property(cluster_id, controller, data, index, dataset1):
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


def scenario2_rule_create_edges(cluster_id, controller, data, index, dataset1):
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

def which_modules_has_been_load(redis_client):
    module_tags = {}
    data = redis_client.execute_command("MODULE LIST")
    for m in data:
        print(m)
        if m[b'name'].decode('utf-8') == "rxGraphdb":
            module_tags["rxGraphdb"] = True
        if m[b'name'].decode('utf-8') == "rxMercator":
            module_tags["rxMercator"] = True
        if m[b'name'].decode('utf-8') == "rxIndexStore":
            module_tags["rxIndexStore"] = True
        if m[b'name'].decode('utf-8') == "rxIndexer":
            module_tags["rxIndexer"] = True
        if m[b'name'].decode('utf-8') == "RXQUERY":
            module_tags["rxQuery"] = True
        if m[b'name'].decode('utf-8') == "rxRule":
            module_tags["rxRule"] = True
    return module_tags


def get_redis_path(redis_client):
    info = redis_client.info("SERVER")
    segments = info["executable"].split('/')
    path = '/'.join(segments[0:len(segments)-2])
    return path


def prepare_controller():
    redis_client = redis.StrictRedis('localhost', 6379, 0)
    redis_path = get_redis_path(redis_client)

    module_config = which_modules_has_been_load(redis_client)
    if not "rxMercator" in module_config:
        r = redis_client.execute_command(
            "MODULE LOAD {}/extensions/src/rxMercator.so".format(redis_path))
        print("rxMercator loaded")
        r = redis_client.execute_command(
            "mercator.add.server localhost 192.168.1.180 6GB 2")
        print("rxMercator server added")
    return redis_client


def connect_to_redis(node):
    redis_client = redis.StrictRedis(node["ip"], node["port"], 0)
    return redis_client


def create_cluster(redis_client):
    cluster_id = redis_client.execute_command("mercator.create.cluster", "::1")
    redis_client.execute_command("mercator.start.cluster", cluster_id)
    cluster_info = json.loads(redis_client.execute_command(
        "mercator.info.cluster", cluster_id).decode('utf-8'))
    info = {"cluster_id": cluster_id}
    for node in cluster_info["nodes"]:
        if node["role"] == "data":
            info["data"] = node
        elif node["role"] == "index":
            info["index"] = node
    return info


def runcmd(cmd, verbose=False, *args, **kwargs):
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        shell=True
    )
    std_out, std_err = process.communicate()
    if verbose:
        print(std_out.strip(), std_err)
    pass


def download_testdata(redis_client, fname):
    path = get_redis_path(redis_client)
    # runcmd("rm -rf {}/data/*.*".format(path), verbose = True)
    runcmd("wget  --timestamping  -P {}/extensions/sys_test/data https://roxoft.dev/assets/{}".format(path, fname), verbose=True)
    # open text file in read mode
    text_file = open("{}/extensions/sys_test/data/{}".format(path, fname), "r")
    data = text_file.read()
    text_file.close()
    data = data.replace("\n", "|").replace("\t", ";")
    return data


def main():
    dataset1 = ''
    redis_path = ''
    controller = prepare_controller()
    cluster_info = create_cluster(controller)
    data_node = connect_to_redis(cluster_info["data"])
    index_node = connect_to_redis(cluster_info["data"])

    dataset1 = download_testdata(controller, "dataset1.txt")
    cluster_id = cluster_info["cluster_id"].decode('utf-8')
    for n in range(0,10):
        system('clear')
        scenario1_as_string(cluster_id, controller,
                            data_node, index_node, dataset1)
        scenario1_as_text(cluster_id, controller,
                          data_node, index_node, dataset1)
        scenario1_as_json(cluster_id, controller,
                          data_node, index_node, dataset1)
        scenario1_as_hash(cluster_id, controller,
                          data_node, index_node, dataset1)
        scenario2_rule_integrity_check(
            cluster_id, controller, data_node, index_node, dataset1)
        scenario2_rule_age_check_add_property(
            cluster_id, controller, data_node, index_node, dataset1)
        scenario2_rule_create_edges(
            cluster_id, controller, data_node, index_node, dataset1)

    index_node.close()
    data_node.close()
    controller.close()


if __name__ == "__main__":
    has_exception = False
    nfails = 0
    main()
