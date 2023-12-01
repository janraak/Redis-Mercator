from __future__ import print_function
from colorama import Fore, Back, Style
import time
import redis
import json

from match import Matcher, HeaderOnly, find_local_ip, filterRows, stripField, ReconnectException

def reconnect(connection):
    cluster_info = json.loads(connection["controller"].execute_command(
        "mercator.info.cluster", connection["cluster_id"]).decode('utf-8'))
    print("After reconnect: {}".format(cluster_info))
    for node in cluster_info["nodes"]:
        if node["role"] == "data":
            print((Style.RESET_ALL+Fore.YELLOW + Back.CYAN + "DATA: {}:{}"+Style.RESET_ALL).format(node["ip"], node["port"]))
            connection["data"] = redis.StrictRedis(node["ip"], node["port"], 0)
            modules = connection['data'].execute_command("info modules")
            print("Data: {}".format(modules))
        elif node["role"] == "index":
            print((Style.RESET_ALL+Fore.YELLOW + Back.CYAN + "INDEX: {}:{}"+Style.RESET_ALL).format(node["ip"], node["port"]))
            connection["index"] = redis.StrictRedis(node["ip"], node["port"], 0)
            modules = connection['index'].execute_command("info modules")
            print("Index: {}".format(modules))
    return connection


def SD_910_Upgrade_Downgrade_setup(cluster_id, controller, data, index):
    matcher = Matcher("SD_910_Upgrade_Downgrade_setup")
    ip = find_local_ip()
    controller.execute_command("mercator.add.server localhost_extra {} 6GB BASE 6383 200".format(ip))

    info = controller.execute_command("mercator.cluster.info", cluster_id);
    matcher.by_hashed("Has cluster info", len(info) > 0 , True)
    matcher.by_sets("check required versions", filterRows(
                                                                        stripField(
                                                                            controller.execute_command("mercator.redis.status")
                                                                            , [b'Address', b'Scope']
                                                                            ), 
                                                                    b'Version', 
                                                                    [b'6.0.20', b'6.2.0', b'7.0.11']
                                                                     ), 
                          [[b'Version', b'7.0.11', b'Redis', b'build', b'rxMercator', b'build'], 
                           [b'Version', b'6.2.0', b'Redis', b'build', b'rxMercator', b'build'], 
                           [b'Version', b'6.0.20', b'Redis', b'build', b'rxMercator', b'build']])
    matcher.by_strings("DOWNLOAD test data", data.execute_command("g.wget", "https://roxoft.dev/assets/bedrock1.txt"), b'bedrock1.txt')
    matcher.by_strings("LOAD test data", data.execute_command("g.set", "FILE", "bedrock1.txt"), b'OK')
    matcher.finalize()
        
#
# This test verifies the VERTEX and EDGE type tree features#
def Regrade_And_Check_Sanity(step, version, connection):
    matcher = connection["Matcher"]
    matcher.by_strings("{}.A.  mercator.upgrade.cluster REDIS {}".format(step, version), connection["controller"].execute_command("mercator.upgrade.cluster", connection["cluster_id"], version), 
                          ["*is now running Redis version {}".format(version), b'Request timedout'])


    connection = reconnect(connection)
    matcher.by_hashed("{}.B.  REDIS {} DATA".format(step, version), connection["data"].execute_command("info", "server")["redis_version"], version)
    matcher.by_hashed("{}.C.  REDIS {} INDEX".format(step, version), connection["index"].execute_command("info", "server")["redis_version"], version)

    matcher.by_hashed("{}.D.  REDIS {} KEYS".format(step, version), connection["data"].execute_command("keys", "*"), connection["keys"])
    matcher.by_hashed("{}.E.  REDIS {} MATCH".format(step, version), HeaderOnly(connection["data"].execute_command("RXGET", "G:match(fred,barney)")), connection["sanity"])
    return connection


def SD_910_Upgrade_Downgrade(cluster_id, controller, data, index):
    matcher = Matcher("SD_910_Upgrade_Downgrade")

    connection = {
        "Matcher": matcher,
        "data": data, 
        "index": index, 
        "controller": controller, 
        "cluster_id":cluster_id, 
        "succes_tally": 0, 
        "sanity": HeaderOnly(data.execute_command("g", "match(fred,barney)")), 
        "keys": data.execute_command("keys", "*")}
    

    connection = Regrade_And_Check_Sanity(1,"6.0.20", connection)
    connection = Regrade_And_Check_Sanity(2,"6.2.0", connection)
    connection = Regrade_And_Check_Sanity(10,"6.0.19", connection)
    connection = Regrade_And_Check_Sanity(11,"6.0.9", connection)

    matcher.finalize()

    raise ReconnectException()

def start_install(version, controller, matcher):
    matcher.by_strings(
        "mercator.redis.install",
        controller.execute_command("mercator.redis.install", version),
        "b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'",
    )

def SD_900_Mercator_Check_Software(cluster_id, controller, data, index):
    # return
    matcher = Matcher("SD_900_Mercator_Check_Software")

    # matcher.by_hashed("mercator.redis.status", filterRows(
    #                                                                     stripField(
    #                                                                         controller.execute_command("mercator.redis.status")
    #                                                                         , b'Address'
    #                                                                         ), 
    #                                                                 b'Version', 
    #                                                                 [b'6.0.19', b'6.0.20', b'6.2.0', b'7.0.11', b'7.0.14', b'7.2.3']
    #                                                                  ), 
    #                       stripField([b'Version', b'7.0.11', b'Scope', b'', b'Redis', b'build', b'rxMercator', b'build', b'Version', b'7.2.3', b'Scope', b'', b'Redis', b'build', b'rxMercator', b'build', b'Version', b'6.2.0', b'Scope', b'', b'Redis', b'build', b'rxMercator', b'build', b'Version', b'6.0.19', b'Scope', b'', b'Redis', b'build', b'rxMercator', b'build', b'Version', b'7.0.14', b'Scope', b'', b'Redis', b'build', b'rxMercator', b'build', b'Version', b'6.0.20', b'Scope', b'', b'Redis', b'build', b'rxMercator', b'build'], b'Address'))
  
    # # start_install("4.0.14", controller, matcher)
    # # start_install("5.0.9", controller, matcher)
    # # start_install("6.0.9", controller, matcher)
    # # start_install("6.0.18", controller, matcher)
    # start_install("6.0.19", controller, matcher)
    # start_install("6.0.20", controller, matcher)
    # start_install("6.2.0", controller, matcher)
    # # start_install("6.2.1", controller, matcher)
    # # start_install("7.0.1", controller, matcher)
    # start_install("7.0.14", controller, matcher)
    # start_install("7.2.3", controller, matcher)
    # # start_install("beta-9", controller, matcher)
    
    matcher.finalize()
