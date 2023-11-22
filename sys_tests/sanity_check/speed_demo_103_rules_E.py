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
# This test verifies the operation of rules with traversals
#
# The rule:
# * Buyers under 21 are prohited to be alcoholic beverages
# 
# The rule works on sales orders
#    The purchased product is tested for alcohol, using a traverse from the Sale to the Product and Back.
#    Since the product may be on many orders we AND the result with our working set to limit our scope to modified keys only.
#
#    An age check is performed on the buyer, using a traverse from the Sale to the Buyer and Back.
#    Since the buyer may have multiple sales , the any orders we AND the result with our working set to limit our scope to modified keys only.
#

def SD_103_Business_Rules_Data_Augmentation_Verify_Traverse_Directions(cluster_id, controller, data, index):
    # pdb.set_trace()
    reset_match_calls()
    succes_tally = 0
    succes_tally += match("KEYS", data.execute_command("keys", "*"), "{}".format([]))
    succes_tally += match("KEYS", index.execute_command("keys", "*"), "{}".format([]))

    succes_tally += match("rule list", data.execute_command("RULE.LIST"), 
                          "{}".format(b'No rules defined'))

    succes_tally += match("get dataset",data.execute_command("g.wget", "https://roxoft.dev/assets/dataset2.txt").decode(), 
                          'dataset2.txt')
    succes_tally += match("add rule linkFather",data.execute_command("RULE.SET", "linkFather", "(haslabel(father).addE(@,father,father_of))" ).decode(), 
                          'Rule for: linkFather Expression: Expression appears to be incorrect!')
    succes_tally += match("add rule linkMother",data.execute_command("RULE.SET", "linkMother", "(haslabel(mother).addE(@,mother,mother_of))" ).decode(), 
                          'Rule for: linkMother Expression: Expression appears to be incorrect!')
    succes_tally += match("add rule linkBrother",data.execute_command("RULE.SET", "linkBrother", "(haslabel(brother).addE(@brother,has_brother,brother_of))" ).decode(), 
                          'Rule for: linkBrother Expression: Expression appears to be incorrect!')
    succes_tally += match("add rule linkSister",data.execute_command("RULE.SET", "linkSister", "(haslabel(sister).addE(@sister,has_sister,sister_of))" ).decode(), 
                          'Rule for: linkSister Expression: Expression appears to be incorrect!')
    succes_tally += match("add rule linkSpouse",data.execute_command("RULE.SET", "linkSpouse", "(haslabel(spouse).addE(@,spouse))" ).decode(), 
                          'Rule for: linkSpouse Expression: Expression appears to be incorrect!')
    succes_tally += match("add rule linkFriend",data.execute_command("RULE.SET", "linkFriend", "(haslabel(friend).addE(@,friend))" ).decode(), 
                          'Rule for: linkFriend Expression: Expression appears to be incorrect!')
    succes_tally += match("add rule linkOwner",data.execute_command("RULE.SET", "linkOwner", "(haslabel(owner).addE(@owner,owned_by,owner_of))" ).decode(), 
                          'Rule for: linkOwner Expression: Expression appears to be incorrect!')
    succes_tally += match("add rule linkBoss",data.execute_command("RULE.SET", "linkBoss", "(haslabel(boss).addE(@boss,has_boss,boss_of))" ).decode(), 
                          'Rule for: linkBoss Expression: Expression appears to be incorrect!')
    
    
    succes_tally += match("LOAD TEXT FILE",data.execute_command("LOAD.TEXT", "CRLF", "TAB_SEPARATED", "FROM", "dataset2.txt", "as_hash").decode(),
                           'OK')
    

    
    succes_tally += match("wait on indexing", data.execute_command("rxIndex wait").decode(), 'OK')



    succes_tally += match("verify out (is)mother == Mothers", data.execute_command("rxquery","g:v(person).out(mother)","ranked"), 
                          "{}".format([[b'key', b'betty', b'type', b'H', b'score', b'1.000000'], [b'key', b'flo', b'type', b'H', b'score', b'1.000000'], [b'key', b'pearl', b'type', b'H', b'score', b'1.000000'], [b'key', b'pebbles', b'type', b'H', b'score', b'1.000000'], [b'key', b'wilma', b'type', b'H', b'score', b'1.000000']]))

    succes_tally += match("verify in (has)mother == Children", data.execute_command("rxquery","g:v(person).in(mother)","ranked"), 
                          "{}".format([[b'key', b'bammbamm', b'type', b'H', b'score', b'1.000000'], [b'key', b'barney', b'type', b'H', b'score', b'1.000000'], [b'key', b'chip', b'type', b'H', b'score', b'1.000000'], [b'key', b'pebbles', b'type', b'H', b'score', b'1.000000'], [b'key', b'roxy', b'type', b'H', b'score', b'1.000000'], [b'key', b'wilma', b'type', b'H', b'score', b'1.000000']]))

    succes_tally += match("verify in children who ARE MOTHERS", data.execute_command("rxquery","g:((v().hasout(mother)) (v.hasin(mother)) &).outT(mother)","ranked"), 
                          "{}".format([[b'subject', b'wilma', b'length', b'1', b'object', b'wilma', b'path', [[b'wilma', b'1']]], [b'subject', b'pebbles', b'length', b'1', b'object', b'pebbles', b'path', [[b'pebbles', b'1']]], [b'subject', b'pebbles', b'length', b'1', b'object', b'pebbles', b'path', [[b'pebbles', b'1']]], [b'subject', b'pebbles', b'length', b'1', b'object', b'pebbles', b'path', [[b'pebbles', b'1']]]]))

    if(succes_tally != get_match_calls()): 
        raise AssertionError( "FAILED: SD_103_Business_Rules_Data_Augmentation, {} of {} steps failed".format(get_match_calls() - succes_tally, get_match_calls()))
        