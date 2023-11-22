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

def SD_102_Traversing_TypeTree(cluster_id, controller, data, index):
    # pdb.set_trace()
    reset_match_calls()
    succes_tally = 0
    succes_tally += match("KEYS", data.execute_command("keys", "*"), "{}".format([]))
    succes_tally += match("KEYS", index.execute_command("keys", "*"), "{}".format([]))

    succes_tally += match("rule list", data.execute_command("RULE.LIST"), 
                          "{}".format(b'No rules defined'))

    succes_tally += match("get dataset",data.execute_command("g.wget", "https://roxoft.dev/assets/products.txt").decode(), 
                          'products.txt')
    
    succes_tally += match("LOAD TEXT FILE",data.execute_command("LOAD.TEXT", "CRLF", "TAB_SEPARATED", "FROM", "products.txt", "as_hash").decode(),
                           'OK')
    
    succes_tally += match("type tree whiskies", data.execute_command("rxtypetree whiskies whiskey bourbon whisky ").decode(), '`whiskies')
    
    succes_tally += match("type tree alcoholic", data.execute_command("rxtypetree alcoholic whiskies wine beer liqueur ").decode(), '`alcoholic')

    succes_tally += match("type tree all beverages", data.execute_command("rxtypetree beverages alcoholic water pop energy").decode(), '`beverages')
    
    succes_tally += match("wait on indexing", data.execute_command("rxIndex wait").decode(), 'OK')



    succes_tally += match("verify whiskies", data.execute_command("rxquery","g:v(whiskies)","ranked"), 
                          "{}".format([[b'key', b'w1', b'type', b'H', b'score', b'1.000000'], [b'key', b'w10', b'type', b'H', b'score', b'1.000000'], [b'key', b'w11', b'type', b'H', b'score', b'1.000000'], [b'key', b'w12', b'type', b'H', b'score', b'1.000000'], [b'key', b'w13', b'type', b'H', b'score', b'1.000000'], [b'key', b'w14', b'type', b'H', b'score', b'1.000000'], [b'key', b'w2', b'type', b'H', b'score', b'1.000000'], [b'key', b'w3', b'type', b'H', b'score', b'1.000000'], [b'key', b'w4', b'type', b'H', b'score', b'1.000000'], [b'key', b'w5', b'type', b'H', b'score', b'1.000000'], [b'key', b'w6', b'type', b'H', b'score', b'1.000000'], [b'key', b'w7', b'type', b'H', b'score', b'1.000000'], [b'key', b'w8', b'type', b'H', b'score', b'1.000000'], [b'key', b'w9', b'type', b'H', b'score', b'1.000000']]))

    succes_tally += match("verify alcoholic", data.execute_command("rxquery","g:v(alcoholic)","ranked"), 
                          "{}".format([[b'key', b'beer1', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer2', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer3', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer4', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer5', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer6', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer7', b'type', b'H', b'score', b'1.000000'], [b'key', b'liqueur1', b'type', b'H', b'score', b'1.000000'], [b'key', b'w1', b'type', b'H', b'score', b'1.000000'], [b'key', b'w10', b'type', b'H', b'score', b'1.000000'], [b'key', b'w11', b'type', b'H', b'score', b'1.000000'], [b'key', b'w12', b'type', b'H', b'score', b'1.000000'], [b'key', b'w13', b'type', b'H', b'score', b'1.000000'], [b'key', b'w14', b'type', b'H', b'score', b'1.000000'], [b'key', b'w2', b'type', b'H', b'score', b'1.000000'], [b'key', b'w3', b'type', b'H', b'score', b'1.000000'], [b'key', b'w4', b'type', b'H', b'score', b'1.000000'], [b'key', b'w5', b'type', b'H', b'score', b'1.000000'], [b'key', b'w6', b'type', b'H', b'score', b'1.000000'], [b'key', b'w7', b'type', b'H', b'score', b'1.000000'], [b'key', b'w8', b'type', b'H', b'score', b'1.000000'], [b'key', b'w9', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine1', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine2', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine3', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine4', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine5', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine6', b'type', b'H', b'score', b'1.000000']]))

    succes_tally += match("verify beverages", data.execute_command("rxquery","g:v(beverages)","ranked"), 
                          "{}".format([[b'key', b'beer1', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer2', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer3', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer4', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer5', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer6', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer7', b'type', b'H', b'score', b'1.000000'], [b'key', b'energy1', b'type', b'H', b'score', b'1.000000'], [b'key', b'energy2', b'type', b'H', b'score', b'1.000000'], [b'key', b'energy3', b'type', b'H', b'score', b'1.000000'], [b'key', b'energy4', b'type', b'H', b'score', b'1.000000'], [b'key', b'energy5', b'type', b'H', b'score', b'1.000000'], [b'key', b'energy6', b'type', b'H', b'score', b'1.000000'], [b'key', b'energy7', b'type', b'H', b'score', b'1.000000'], [b'key', b'energy8', b'type', b'H', b'score', b'1.000000'], [b'key', b'liqueur1', b'type', b'H', b'score', b'1.000000'], [b'key', b'soft1', b'type', b'H', b'score', b'1.000000'], [b'key', b'soft2', b'type', b'H', b'score', b'1.000000'], [b'key', b'soft3', b'type', b'H', b'score', b'1.000000'], [b'key', b'soft4', b'type', b'H', b'score', b'1.000000'], [b'key', b'soft5', b'type', b'H', b'score', b'1.000000'], [b'key', b'w1', b'type', b'H', b'score', b'1.000000'], [b'key', b'w10', b'type', b'H', b'score', b'1.000000'], [b'key', b'w11', b'type', b'H', b'score', b'1.000000'], [b'key', b'w12', b'type', b'H', b'score', b'1.000000'], [b'key', b'w13', b'type', b'H', b'score', b'1.000000'], [b'key', b'w14', b'type', b'H', b'score', b'1.000000'], [b'key', b'w2', b'type', b'H', b'score', b'1.000000'], [b'key', b'w3', b'type', b'H', b'score', b'1.000000'], [b'key', b'w4', b'type', b'H', b'score', b'1.000000'], [b'key', b'w5', b'type', b'H', b'score', b'1.000000'], [b'key', b'w6', b'type', b'H', b'score', b'1.000000'], [b'key', b'w7', b'type', b'H', b'score', b'1.000000'], [b'key', b'w8', b'type', b'H', b'score', b'1.000000'], [b'key', b'w9', b'type', b'H', b'score', b'1.000000'], [b'key', b'water1', b'type', b'H', b'score', b'1.000000'], [b'key', b'water2', b'type', b'H', b'score', b'1.000000'], [b'key', b'water3', b'type', b'H', b'score', b'1.000000'], [b'key', b'water4', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine1', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine2', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine3', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine4', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine5', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine6', b'type', b'H', b'score', b'1.000000']]))

    if(succes_tally != get_match_calls()): 
        raise AssertionError( "FAILED: SD_102_Traversing_TypeTree, {} of {} steps failed".format(get_match_calls() - succes_tally, get_match_calls()))
        