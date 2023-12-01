from __future__ import print_function
from colorama import Fore, Back, Style
import time
import redis
import json
import pdb
from match import match, reset_match_calls, get_match_calls

global redis_path
def column(matrix, i):
    return [row[i] for row in matrix]

def reconnect(connection):
    cluster_info = json.loads(connection["controller"].execute_command(
        "mercator.info.cluster", connection["cluster_id"]).decode('utf-8'))
    print((Style.RESET_ALL+Fore.BLUE + Back.CYAN + "{}"+Style.RESET_ALL).format(cluster_info))
    for node in cluster_info["nodes"]:
        if node["role"] == "data":
            print((Style.RESET_ALL+Fore.YELLOW + Back.CYAN + "DATA: {}:{}"+Style.RESET_ALL).format(node["ip"], node["port"]))
            connection["data"] = redis.StrictRedis(node["ip"], node["port"], 0)
        elif node["role"] == "index":
            print((Style.RESET_ALL+Fore.YELLOW + Back.CYAN + "INDEX: {}:{}"+Style.RESET_ALL).format(node["ip"], node["port"]))
            connection["index"] = redis.StrictRedis(node["ip"], node["port"], 0)
    return connection


def SD_910_Upgrade_Downgrade_setup(cluster_id, controller, data, index):
    versions = controller.execute_command("mercator.redis.status")

    for version in versions:
        # print((Style.RESET_ALL+Fore.WHITE + Back.CYAN + "Redis version  {}"+Style.RESET_ALL).format(version))           
        if version  [1] == b'6.0.19' or version  [1] == b'6.0.20' or version  [1] == b'6.2.0' or version  [1] == b'7.0.11' or  version  [1] == b'7.0.14' or  version  [1] == b'7.2.3':  
            if version [7] != b'build' or  version [9] != b'build' :
                print((Style.RESET_ALL+Fore.WHITE + Back.RED + "Redis version not build {}"+Style.RESET_ALL).format(version))           
                raise Exception("MissingVersion")
    print(data.execute_command("g.wget", "https://roxoft.dev/assets/bedrock1.txt"))
    print(data.execute_command("g.set", "FILE", "bedrock1.txt"))
    global path_between_fred_and_barney
    path_between_fred_and_barney = "{}".format(data.execute_command("g", "match(fred,barney)"))
    print(path_between_fred_and_barney)

        
#
# This test verifies the VERTEX and EDGE type tree features#
def Regrade_And_Check_Sanity(step, version, connection):
    succes_tally = connection["succes_tally"]
    succes_tally += match("{}.  mercator.upgrade.cluster REDIS {}".format(step, version), connection["controller"].execute_command("mercator.upgrade.cluster", connection["cluster_id"], version), 
                          "*is now running Redis version {}".format(version))


    connection = reconnect(connection)
    succes_tally += match("{}.  REDIS {}".format(step, version), connection["data"].execute_command("info", "server")["redis_version"], version)
    succes_tally += match("{}.  REDIS {}".format(step, version), connection["index"].execute_command("info", "server")["redis_version"], version)

    succes_tally += match("{}.  REDIS {}".format(step, version), connection["data"].execute_command("g", "match(fred,barney)"), connection["sanity"])
    connection["succes_tally"] = succes_tally
    return connection


def SD_910_Upgrade_Downgrade(cluster_id, controller, data, index):
    reset_match_calls()

    connection = {"data": data, "index": index, "controller": controller, "cluster_id":cluster_id, "succes_tally": 0, "sanity": "{}".format(data.execute_command("g", "match(fred,barney)"))}
    connection = Regrade_And_Check_Sanity(1,"6.0.20", connection)
    connection = Regrade_And_Check_Sanity(2,"6.2.0", connection)
    # connection = Regrade_And_Check_Sanity(3,"7.0.11", connection)
    # connection = Regrade_And_Check_Sanity(4,"7.0.14", connection)
    # connection = Regrade_And_Check_Sanity(5,"7.2.3", connection)
    # connection = Regrade_And_Check_Sanity(6,"7.0.14", connection)
    # connection = Regrade_And_Check_Sanity(7,"7.0.11", connection)
    connection = Regrade_And_Check_Sanity(8,"6.2.0", connection)
    # connection = Regrade_And_Check_Sanity(9,"6.0.20", connection)
    connection = Regrade_And_Check_Sanity(10,"6.0.19", connection)
    connection = Regrade_And_Check_Sanity(11,"6.0.9", connection)

    if(connection["succes_tally"] != get_match_calls()): 
        AssertionError( "FAILED: SD_910_Upgrade_Downgrade, {} of {} steps failed".format(get_match_calls() - connection["succes_tally"], get_match_calls()))
    
    raise  Exception("Reconnect")

def start_install(version, controller):
    return match("mercator.redis.install", controller.execute_command("mercator.redis.install", version), 
                          "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))

def SD_900_Mercator_Check_Software(cluster_id, controller, data, index):
    # return
    # return
    reset_match_calls()
    succes_tally = 0

    succes_tally += match("mercator.redis.status", controller.execute_command("mercator.redis.status"), 
                          "{}".format([[b'Version', b'6.2.13', b'Scope', b'', b'Address', b'192.168.1.200', b'Redis', b'build', b'rxMercator', b'build'], [b'Version', b'7.0.11', b'Scope', b'', b'Address', b'192.168.1.200', b'Redis', b'build', b'rxMercator', b'build'], [b'Version', b'7.2.0', b'Scope', b'', b'Address', b'192.168.1.200', b'Redis', b'build', b'rxMercator', b'build']]))
  
    # succes_tally += start_install("4.0.14", controller)
    # succes_tally += start_install("5.0.9", controller)
    # succes_tally += start_install("6.0.9", controller)
    # succes_tally += start_install("6.0.18", controller)
    # succes_tally += start_install("6.0.19", controller)
    # succes_tally += start_install("6.0.20", controller)
    # succes_tally += start_install("6.2.0", controller)
    # succes_tally += start_install("6.2.1", controller)
    # succes_tally += start_install("7.0.1", controller)
    # succes_tally += start_install("7.0.14", controller)
    # succes_tally += start_install("7.2.3", controller)
    # succes_tally += start_install("beta-9", controller)
    
    if(succes_tally != get_match_calls()): 
        AssertionError( "FAILED: SD_900_Mercator_Check_Software, {} of {} steps failed".format(get_match_calls() - succes_tally, get_match_calls()))
