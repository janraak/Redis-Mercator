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


# match_calls = 0

# def reset_match_calls():
#     global match_calls
#     match_calls = 0

# def get_match_calls():
#     global match_calls
#     return match_calls

# def match(step, outcome, expected):
#     global match_calls
#     match_calls += 1
#     print(Style.RESET_ALL)
#     if("{}".format(outcome) == expected):
#         print (Style.RESET_ALL+"STEP:   {}{}\nOK  :   {}{}".format(Fore.BLUE, step, Fore.GREEN, outcome))
#         return 1
#     else:
#         print (Style.RESET_ALL+"\nSTEP:   {}{}\nFAIL:   {}{}\nEXPECT: {}{}\n".format(Fore.BLUE, step, Fore.RED, outcome, Fore.CYAN, expected))
#         return 0

    
def SD_103_Business_Rules_and_or_Triggers(cluster_id, controller, data, index):
    # pdb.set_trace()
    succes_tally = 0
    reset_match_calls()
    succes_tally += match("get dataset",data.execute_command("g.wget", "https://roxoft.dev/assets/bedrock1.txt").decode(), 
                          'bedrock1.txt')
    succes_tally += match("add rule",data.execute_command("RULE.SET", "age_restriction", "(has(type,Purchase).inout(Purchase).lt(age,21)).property(status,age_check).redis(rpush, agecheck, @graph)").decode(), 
                          'Rule for: age_restriction Expression: Expression appears to be incorrect!')
    succes_tally += match("add persona",data.execute_command("RXQUERY", "G:AddV(bambam,MALE).property(age,9).AddV(fred,MALE).property(age, 47)"), 
                          "{}".format([[b'key', b'fred', b'type', b'H', b'score', b'1.000000']]))
    succes_tally += match("add purchase 1",data.execute_command("RXQUERY", "G:AddV(P00001,Purchase).property(item,amstel).property(category,beer).predicate(Purchase).subject(bambam).object(P00001)"),
                           "{}".format([[b'key', b'Purchase:P00001:bambam', b'type', b'H', b'score', b'1.000000'], [b'key', b'Purchase:bambam:P00001', b'type', b'H', b'score', b'1.000000']]))
    succes_tally += match("add purchase 2",data.execute_command("RXQUERY", "G:AddV(P00002,Purchase).property(item,amstel).property(category,beer).predicate(Purchase).subject(fred).object(P00002)"), 
                          "{}".format([[b'key', b'Purchase:P00002:fred', b'type', b'H', b'score', b'1.000000'], [b'key', b'Purchase:fred:P00002', b'type', b'H', b'score', b'1.000000']]))
    succes_tally += match("backup db","{}".format(data.execute_command("BGSAVE")), "True")
    bambam = data.hgetall("bambam")
    succes_tally += match("check bambam", bambam, "{}".format( {b'type': b'MALE', b'age': b'9'}))
    fred = data.hgetall("fred")
    succes_tally += match("check fred", fred, "{}".format( {b'type': b'MALE', b'age': b'47'}))
    succes_tally += match("wait on indexing", data.execute_command("rxIndex wait").decode(), 'OK')
    agecheck_length = 0
    agecheck_repeats = 0
    while agecheck_length == 0 and agecheck_repeats < 10:
        agecheck_length = data.llen("agecheck")
        agecheck_repeats += 1
        if agecheck_length == 0: time.sleep(1)

    succes_tally += match("rule list", data.execute_command("RULE.LIST"), 
                          "{}".format([[b'name', b'age_restriction', b'rule', b' type Purchase , HAS Purchase INOUT age 21 , LT . . status age_check , PROPERTY rpush agecheck , @graph , REDIS . .', b'no of applies', 10, b'no of skips', 16, b'no of hits', 3, b'no of misses', 7]]))


    succes_tally += match("check queue", data.lrange("agecheck", 0, 1000), 
                          "{}".format([b'{"entities":[{"iri":"bambam", "entity":"vertice","type": "MALE","age": "9","status": "age_check"},{"iri":"P00001", "entity":"vertice","type": "Purchase","item": "amstel","category": "beer"}]", triplets":[]}', b'{"entities":[{"iri":"bambam", "entity":"vertice","type": "MALE","age": "9","status": "age_check"},{"iri":"P00001", "entity":"vertice","type": "Purchase","item": "amstel","category": "beer"}]", triplets":[]}', b'{"entities":[{"iri":"bambam", "entity":"vertice","type": "MALE","age": "9","status": "age_check"},{"iri":"P00001", "entity":"vertice","type": "Purchase","item": "amstel","category": "beer"}]", triplets":[]}']))

    if(succes_tally !=     get_match_calls()): 
        raise AssertionError( "FAILED: SD_103_Business_Rules_and_or_Triggers, {} of {} steps failed".format(get_match_calls() - succes_tally, get_match_calls()))

#
# This test verifies the operation of rules with traversals
#
# The rule:
# * Buyers under 21 are prohited to be alcoholic beverages
#

def SD_103_Business_Rules_and_or_Triggers_with_Traversal(cluster_id, controller, data, index):
    # pdb.set_trace()
    reset_match_calls()
    succes_tally = 0
    succes_tally += match("get dataset",data.execute_command("g.wget", "https://roxoft.dev/assets/bedrock1.txt").decode(), 
                          'bedrock1.txt')
    succes_tally += match("add rule",data.execute_command("RULE.SET", "age_restriction", ".v(Purchase).as(order).in(Purchase).has(type,beer).use(order).in(Buyer).LT(age,21).property(verification,age).inout(Buyer).as(breach).redis(rpush,kettle2,@graph).drop().reset().use(breach)").decode(), 
                          'Rule for: age_restriction Expression: Expression appears to be incorrect!')
    succes_tally += match("add persona",data.execute_command("RXQUERY", "G:AddV(bambam,MALE).property(age,9).AddV(fred,MALE).property(age, 47).AddV(barney,MALE).property(age, 44)"), 
                          "{}".format([[b'key', b'barney', b'type', b'H', b'score', b'1.000000']]))
    succes_tally += match("link father/son",data.execute_command("RXQUERY", "G:predicate(father_of).subject(barney).object(bambam).predicate(son_of).subject(bambam).object(barney)"),
                           "{}".format([[b'key', b'son_of:bambam:barney', b'type', b'H', b'score', b'1.000000'], [b'key', b'son_of:bambam:father_of:bambam:barney', b'type', b'H', b'score', b'1.000000'], [b'key', b'son_of:bambam:father_of:barney:bambam', b'type', b'H', b'score', b'1.000000'], [b'key', b'son_of:barney:bambam', b'type', b'H', b'score', b'1.000000'], [b'key', b'son_of:father_of:bambam:barney:bambam', b'type', b'H', b'score', b'1.000000'], [b'key', b'son_of:father_of:barney:bambam:bambam', b'type', b'H', b'score', b'1.000000']]))
    
    succes_tally += match("add products",data.execute_command("RXQUERY", "G:AddV(BottledWater,nonalcoholic).AddV(7up,nonalcoholic).AddV(RedBull,sports).AddV(Amstel,beer)"), 
                          "{}".format([[b'key', b'Amstel', b'type', b'H', b'score', b'1.000000']]))
    
    succes_tally += match("add purchase 1.1",data.execute_command("RXQUERY", "G:AddV(P00001,Purchase).predicate(Buyer).subject(bambam).object(P00001).predicate(Item).subject(Amstel).object(P00001)"),
                           "{}".format([[b'key', b'Item:Amstel:Buyer:P00001:bambam', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:Amstel:Buyer:bambam:P00001', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:Amstel:P00001', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:Buyer:P00001:bambam:Amstel', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:Buyer:bambam:P00001:Amstel', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:P00001:Amstel', b'type', b'H', b'score', b'1.000000']]))
    
    succes_tally += match("add purchase 1.2",data.execute_command("RXQUERY", "G:AddV(P00001,Purchase).predicate(Buyer).subject(bambam).object(P00001).predicate(Item).subject(Amstel).object(P00001)"),
                           "{}".format([[b'key', b'Item:Amstel:Buyer:P00001:bambam', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:Amstel:Buyer:bambam:P00001', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:Amstel:P00001', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:Buyer:P00001:bambam:Amstel', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:Buyer:bambam:P00001:Amstel', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:P00001:Amstel', b'type', b'H', b'score', b'1.000000']]))
    
    succes_tally += match("add purchase 2.1",data.execute_command("RXQUERY", "G:AddV(P00002,Purchase).predicate(Buyer).subject(fred).object(P00002).predicate(Item).subject(Amstel).object(P00002)"), 
                          "{}".format([[b'key', b'Item:Amstel:Buyer:P00002:fred', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:Amstel:Buyer:fred:P00002', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:Amstel:P00002', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:Buyer:P00002:fred:Amstel', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:Buyer:fred:P00002:Amstel', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:P00002:Amstel', b'type', b'H', b'score', b'1.000000']]))
    
    succes_tally += match("add purchase 2.2",data.execute_command("RXQUERY", "G:AddV(P00002,Purchase).predicate(Buyer).subject(fred).object(P00002).predicate(Item).subject(RedBull).object(P00002)"), 
                          "{}".format([[b'key', b'Item:Buyer:P00002:fred:RedBull', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:Buyer:fred:P00002:RedBull', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:P00002:RedBull', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:RedBull:Buyer:P00002:fred', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:RedBull:Buyer:fred:P00002', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:RedBull:P00002', b'type', b'H', b'score', b'1.000000']]))
    
    succes_tally += match("backup db","{}".format(data.execute_command("BGSAVE")), "True")
    bambam = data.hgetall("bambam")
    succes_tally += match("check bambam", bambam, "{}".format( {b'type': b'MALE', b'age': b'9'}))
    fred = data.hgetall("fred")
    succes_tally += match("check fred", fred, "{}".format( {b'type': b'MALE', b'age': b'47'}))
    succes_tally += match("wait on indexing", data.execute_command("rxIndex wait").decode(), 'OK')


    agecheck_length = 0
    agecheck_repeats = 0
    while agecheck_length == 0 and agecheck_repeats < 10:
        agecheck_length = data.llen("kettle2")
        agecheck_repeats += 1
        if agecheck_length == 0: time.sleep(1)

    succes_tally += match("validate age check", data.execute_command("RXGET", "g:v().has(verification,age)"), 
                          "{}".format([[b'key', b'bambam', b'score', b'1.000000', b'value', [b'type', b'MALE', b'age', b'9', b'verification', b'age']]]))

    succes_tally += match("rule list", data.execute_command("RULE.LIST"), 
                          "{}".format([[b'name', b'age_restriction', b'rule', b' Purchase V order AS Purchase IN type beer , HAS order USE Buyer IN age 21 , LT verification age , PROPERTY Buyer INOUT breach AS rpush kettle2 , @graph , REDIS DROP RESET breach USE . . . . . . . . . . . . . .', b'no of applies', 12, b'no of skips', 96, b'no of hits', 1, b'no of misses', 11]]))


    succes_tally += match("check queue", data.lrange("kettle2", 0, 1000), 
                          "{}".format( [b'{"entities":[{"iri":"P00001", "entity":"vertice","type": "Purchase"},{"iri":"Amstel", "entity":"vertice","type": "beer"},{"iri":"bambam", "entity":"vertice","type": "MALE","age": "9","verification": "age"}]", triplets":[{"predicate":"Buyer:P00001:bambam", "subject":"bambam", "object":"Item:Buyer:P00001:bambam:Amstel", "depth":1},{"predicate":"bambam", "subject":"son_of:bambam:barney", "object":"father_of:bambam:barney", "depth":2}]}']))

    if(succes_tally != get_match_calls()): 
        raise AssertionError( "FAILED: SD_103_Business_Rules_and_or_Triggers, {} of {} steps failed".format(get_match_calls() - succes_tally, get_match_calls()))
        