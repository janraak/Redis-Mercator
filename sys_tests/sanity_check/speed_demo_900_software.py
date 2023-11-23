from __future__ import print_function
# from colorama import Fore, Back, Style
import time
import redis
import json
import pdb
from match import match, reset_match_calls, get_match_calls

global redis_path
def column(matrix, i):
    return [row[i] for row in matrix]
          
#
# This test verifies the VERTEX and EDGE type tree features#

def SD_900_Mercator_Check_Software(cluster_id, controller, data, index):
    reset_match_calls()
    succes_tally = 0

    succes_tally += match("mercator.redis.status", controller.execute_command("mercator.redis.status"), 
                          "{}".format([[b'Version', b'6.2.13', b'Scope', b'', b'Address', b'192.168.1.200', b'Redis', b'build', b'rxMercator', b'build'], [b'Version', b'7.0.11', b'Scope', b'', b'Address', b'192.168.1.200', b'Redis', b'build', b'rxMercator', b'build'], [b'Version', b'7.2.0', b'Scope', b'', b'Address', b'192.168.1.200', b'Redis', b'build', b'rxMercator', b'build']]))
  
    succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "5.0.9"), 
                          "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))
  
    succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "6.0.1"), 
                          "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))
    
    succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "6.2.0"), 
                          "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))
    
    succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "6.2.1"), 
                          "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))
  
    succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "7.0.1"), 
                          "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))
  
    succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "7.0.14"), 
                          "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))
    
    succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "7.2.3"), 
                          "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))
    
    succes_tally += match("mercator.redis.install", controller.execute_command("mercator.redis.install", "beta-9"), 
                          "{}".format(b'OK, Software installation started, use the mercator.redis.status command to verify the installation.'))
    
    succes_tally += match("mercator.redis.status", controller.execute_command("mercator.redis.status"), 
                          "{}".format([[b'Version', b'6.2.13', b'Scope', b'', b'Address', b'192.168.1.200', b'Redis', b'build', b'rxMercator', b'build'], [b'Version', b'7.0.11', b'Scope', b'', b'Address', b'192.168.1.200', b'Redis', b'build', b'rxMercator', b'build'], [b'Version', b'7.2.0', b'Scope', b'', b'Address', b'192.168.1.200', b'Redis', b'build', b'rxMercator', b'build']]))

    if(succes_tally != get_match_calls()): 
        raise AssertionError( "FAILED: SD_900_Mercator_Check_Software, {} of {} steps failed".format(get_match_calls() - succes_tally, get_match_calls()))
        