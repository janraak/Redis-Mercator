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

def reconnect(cluster_id, controller, data, index):
    cluster_info = json.loads(redis_client.execute_command(
        "mercator.info.cluster", cluster_id).decode('utf-8'))
    print((Style.RESET_ALL+Fore.BLUE + Back.CYAN + "{}"+Style.RESET_ALL).format(cluster_info))
    info = {"cluster_id": cluster_id}
    connection ={}
    for node in cluster_info["nodes"]:
        if node["role"] == "data":
            info["data"] = node
            connection["data"]: redis.StrictRedis(node["ip"], node["port"], 0)
        elif node["role"] == "index":
            info["index"] = node
            connection["index"] =redis.StrictRedis(node["ip"], node["port"], 0)
    return connection


def SD_910_Upgrade_Degrade_setup(cluster_id, controller, data, index):
    versions = controller.execute_command("mercator.redis.status")

    # pdb.set_trace()
    for version in versions:
        # print((Style.RESET_ALL+Fore.WHITE + Back.CYAN + "Redis version  {}"+Style.RESET_ALL).format(version))           
        if version  [1] == b'6.0.20' or version  [1] == b'6.2.0' or version  [1] == b'7.0.11' or  version  [1] == b'7.0.14' or  version  [1] == b'7.2.3':  
            if version [7] != b'build' or  version [9] != b'build' :
                print((Style.RESET_ALL+Fore.WHITE + Back.RED + "Redis version not build {}"+Style.RESET_ALL).format(version))           
                raise Exception("MissingVersion")
    print(data.execute_command("g.wget", "https://roxoft.dev/assets/bedrock1.txt"))
    print(data.execute_command("g.set", "FILE", "bedrock1.txt"))
    global path_between_fred_and_barney
    # path_between_fred_and_barney = ""
    path_between_fred_and_barney = "{}".format(data.execute_command("g", "match(fred,barney)"))
    print(path_between_fred_and_barney)

        
#
# This test verifies the VERTEX and EDGE type tree features#

def SD_910_Upgrade_Degrade(cluster_id, controller, data, index):
    reset_match_calls()
    succes_tally = 0
    # pdb.set_trace()

    succes_tally += match("mercator.upgrade.cluster", controller.execute_command("mercator.upgrade.cluster", cluster_id, "6.0.20"), 
                          "Cluster {} upgraded to Redis {}".format(cluster_id, "7.2.3"))

    pdb.set_trace()

    reconnection = reconnect(cluster_id, controller, data, index)
    data=reconnection["data"]
    index=reconnection["index"]
    info =data.execute_command("info", "server")
    succes_tally += match("REDIS 6.0.20", info["redis_version"], "6.0.20")

    succes_tally += match("REDIS 6.0.20", data.execute_command("g", "match(fred,barney)"), 
                          path_between_fred_and_barney)

    succes_tally += match("mercator.upgrade.cluster", controller.execute_command("mercator.upgrade.cluster", cluster_id, "6.2.0"), 
                          "Cluster {} upgraded to Redis {}".format(cluster_id, "7.2.3"))

    reconnection = reconnect(cluster_id, controller, data, index)
    data=reconnection["data"]
    index=reconnection["index"]

    info =data.execute_command("info", "server")
    succes_tally += match("REDIS 6.2.0", info["redis_version"], "6.2.0")

    succes_tally += match("REDIS 6.2.0", data.execute_command("g", "match(fred,barney)"), 
                          path_between_fred_and_barney)


    succes_tally += match("REDIS 6.2.0", data.execute_command("info", "server"), 
                          "6.2.0")


    if(succes_tally != get_match_calls()): 
        AssertionError( "FAILED: SD_900_Mercator_Check_Software, {} of {} steps failed".format(get_match_calls() - succes_tally, get_match_calls()))
    
    # raise  Exception("Reconnect")

def SD_900_Mercator_Check_Software(cluster_id, controller, data, index):
    # return
    reset_match_calls()
    succes_tally = 0

    succes_tally += match("mercator.redis.status", controller.execute_command("mercator.redis.status"), 
                          "{}".format([[b'Version', b'6.2.13', b'Scope', b'', b'Address', b'192.168.1.200', b'Redis', b'build', b'rxMercator', b'build'], [b'Version', b'7.0.11', b'Scope', b'', b'Address', b'192.168.1.200', b'Redis', b'build', b'rxMercator', b'build'], [b'Version', b'7.2.0', b'Scope', b'', b'Address', b'192.168.1.200', b'Redis', b'build', b'rxMercator', b'build']]))
  
    # succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "4.0.14"), 
    #                       "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))
  
    # succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "5.0.9"), 
    #                       "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))
  
    succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "6.0.19"), 
                          "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))
    
    # succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "6.0.20"), 
    #                       "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))
    
    # succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "6.2.0"), 
    #                       "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))
    
    # succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "6.2.1"), 
    #                       "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))
  
    # succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "7.0.1"), 
    #                       "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))
  
    # succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "7.0.1"), 
    #                       "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))
  
    # succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "7.0.14"), 
    #                       "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))
    
    # succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "7.2.3"), 
    # succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "7.2.3"), 
    #                       "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))
    
    # # succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "beta-9"), 
    # #                       "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))    

    # # succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "beta-9"), 
    # #                       "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))
    
    # succes_tally += match("mercator.redis.status", controller.execute_command("mercator.redis.status"), 
    #                       "{}".format([[b'Version', b'6.2.13', b'Scope', b'', b'Address', b'192.168.1.200', b'Redis', b'build', b'rxMercator', b'build'], [b'Version', b'7.0.11', b'Scope', b'', b'Address', b'192.168.1.200', b'Redis', b'build', b'rxMercator', b'build'], [b'Version', b'7.2.0', b'Scope', b'', b'Address', b'192.168.1.200', b'Redis', b'build', b'rxMercator', b'build']]))

    # succes_tally += match("mercator.upgrade.cluster", controller.execute_command("mercator.upgrade.cluster", cluster_id, "6.0.20"), 
    #                       "Cluster {} upgraded to Redis {}".format(cluster_id, "7.2.3"))

    # succes_tally += match("mercator.upgrade.cluster", controller.execute_command("mercator.upgrade.cluster", cluster_id, "6.0.20"), 
    #                       "Cluster {} upgraded to Redis {}".format(cluster_id, "7.2.3"))

    # if(succes_tally != get_match_calls()): 
    #     AssertionError( "FAILED: SD_900_Mercator_Check_Software, {} of {} steps failed".format(get_match_calls() - succes_tally, get_match_calls()))
    
    # raise  Exception("Reconnect")
        