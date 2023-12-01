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

def SD_103_Business_Rules_and_or_Triggers(cluster_id, controller, data, index):
    succes_tally = 0
    reset_match_calls()
    succes_tally += match("get dataset",data.execute_command("g.wget", "https://roxoft.dev/assets/bedrock1.txt").decode(), 
                          'bedrock1.txt')
    succes_tally += match("add rule",data.execute_command("RULE.SET", "age_restriction", "(has(type,Sale).inout(Sale).lt(age,21)).property(status,age_check).redis(rpush, agecheck, @graph)").decode(), 
                          'Rule for: age_restriction Expression: Expression appears to be incorrect!')
    succes_tally += match("add persona",data.execute_command("RXQUERY", "G:AddV(bambam,MALE).property(age,9).AddV(fred,MALE).property(age, 47)"), 
                          "{}".format([[b'key', b'fred', b'type', b'H', b'score', b'1.000000']]))
    succes_tally += match("add purchase 1",data.execute_command("RXQUERY", "G:AddV(P00001,Sale).property(item,amstel).property(category,beer).predicate(Sale).subject(bambam).object(P00001)"),
                           "{}".format([[b'key', b'Sale:P00001:bambam', b'type', b'H', b'score', b'1.000000'], [b'key', b'Sale:bambam:P00001', b'type', b'H', b'score', b'1.000000']]))
    succes_tally += match("add purchase 2",data.execute_command("RXQUERY", "G:AddV(P00002,Sale).property(item,amstel).property(category,beer).predicate(Sale).subject(fred).object(P00002)"), 
                          "{}".format([[b'key', b'Sale:P00002:fred', b'type', b'H', b'score', b'1.000000'], [b'key', b'Sale:fred:P00002', b'type', b'H', b'score', b'1.000000']]))
    succes_tally += match("backup db","{}".format(data.execute_command("BGSAVE")), "True")
    bambam = data.hgetall("bambam")
    succes_tally += match("check bambam", bambam, "{}".format( {b'type': b'MALE', b'age': b'9'}))
    fred = data.hgetall("fred")
    succes_tally += match("check fred", fred, "{}".format( {b'type': b'MALE', b'age': b'47'}))
    succes_tally += match("wait on indexing", data.execute_command("rxIndex wait").decode(), 'OK')

    succes_tally += match("rule list", data.execute_command("RULE.LIST"), 
                          "{}".format([[b'name', b'age_restriction', b'rule', b' type Sale , HAS Sale INOUT age 21 , LT . . status age_check , PROPERTY rpush agecheck , @graph , REDIS . .', b'no of applies', 10, b'no of skips', 16, b'no of hits', 3, b'no of misses', 7]]))


    succes_tally += match("check queue", data.lrange("agecheck", 0, 1000), 
                          "{}".format([b'{"entities":[{"iri":"bambam", "entity":"vertice","type": "MALE","age": "9","status": "age_check"},{"iri":"P00001", "entity":"vertice","type": "Sale","item": "amstel","category": "beer"}]", triplets":[]}', b'{"entities":[{"iri":"bambam", "entity":"vertice","type": "MALE","age": "9","status": "age_check"},{"iri":"P00001", "entity":"vertice","type": "Sale","item": "amstel","category": "beer"}]", triplets":[]}', b'{"entities":[{"iri":"bambam", "entity":"vertice","type": "MALE","age": "9","status": "age_check"},{"iri":"P00001", "entity":"vertice","type": "Sale","item": "amstel","category": "beer"}]", triplets":[]}']))

    if(succes_tally !=     get_match_calls()): 
        raise AssertionError( "FAILED: SD_103_Business_Rules_and_or_Triggers, {} of {} steps failed".format(get_match_calls() - succes_tally, get_match_calls()))

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

def SD_103_Business_Rules_and_or_Triggers_with_Traversal(cluster_id, controller, data, index):
    # pdb.set_trace()
    reset_match_calls()
    succes_tally = 0
    succes_tally += match("KEYS", data.execute_command("keys", "*"), "{}".format([]))
    succes_tally += match("KEYS", index.execute_command("keys", "*"), "{}".format([]))

    succes_tally += match("rule list", data.execute_command("RULE.LIST"), 
                          "{}".format(b'No rules defined'))

    succes_tally += match("get dataset",data.execute_command("g.wget", "https://roxoft.dev/assets/bedrock1.txt").decode(), 
                          'bedrock1.txt')
    succes_tally += match("add rule",data.execute_command("RULE.SET", "age_restriction", "has(type,sale).as(workset).(in(Item).has(type,beer).out(Item).use(workset) & ) " 
                          ".(in(Buyer).LT(age,21).property(verification,age).out(Buyer).use(workset) &)" 
                          ".as(breach).redis(rpush,kettle2,@graph).drop().reset().use(breach))").decode(), 
                          'Rule for: age_restriction Expression: Expression appears to be incorrect!')
    succes_tally += match("add persona",data.execute_command("RXQUERY", "G:AddV(bambam,MALE).property(age,9).AddV(fred,MALE).property(age, 47).AddV(barney,MALE).property(age, 44)"), 
                          "{}".format([[b'key', b'barney', b'type', b'H', b'score', b'1.000000']]))
    succes_tally += match("link father/son",data.execute_command("RXQUERY", "G:predicate(father_of).subject(barney).object(bambam).predicate(son_of).subject(bambam).object(barney)"),
                           "{}".format([[b'key', b'son_of:bambam:barney', b'type', b'H', b'score', b'1.000000'], [b'key', b'son_of:bambam:father_of:bambam:barney', b'type', b'H', b'score', b'1.000000'], [b'key', b'son_of:bambam:father_of:barney:bambam', b'type', b'H', b'score', b'1.000000'], [b'key', b'son_of:barney:bambam', b'type', b'H', b'score', b'1.000000'], [b'key', b'son_of:father_of:bambam:barney:bambam', b'type', b'H', b'score', b'1.000000'], [b'key', b'son_of:father_of:barney:bambam:bambam', b'type', b'H', b'score', b'1.000000']]))
    
    succes_tally += match("add products",data.execute_command("RXQUERY", "G:AddV(BottledWater,nonalcoholic).AddV(sevenUp,nonalcoholic).AddV(RedBull,sports).AddV(Amstel,beer)"), 
                          "{}".format([[b'key', b'Amstel', b'type', b'H', b'score', b'1.000000']]))
    
    succes_tally += match("add purchase 0.0",data.execute_command("RXQUERY", "G:AddV(P00000,Sale).predicate(Buyer).subject(bambam).object(P00000)"),
                           "{}".format([[b'key', b'Buyer:P00000:bambam', b'type', b'H', b'score', b'1.000000'], [b'key', b'Buyer:bambam:P00000', b'type', b'H', b'score', b'1.000000']]))
    
    succes_tally += match("add purchase 0.1",data.execute_command("RXQUERY", "G:predicate(Item).subject(Amstel).object(P00000)"),
                           "{}".format([[b'key', b'Item:Amstel:P00000', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:P00000:Amstel', b'type', b'H', b'score', b'1.000000']]))
    
    succes_tally += match("add purchase 0.2",data.execute_command("RXQUERY", "G:predicate(Item).subject(BottledWater).object(P00000)"), 
                          "{}".format([[b'key', b'Item:BottledWater:P00000', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:P00000:BottledWater', b'type', b'H', b'score', b'1.000000']]))
    
    succes_tally += match("add purchase 1.0",data.execute_command("RXQUERY", "G:AddV(P00001,Sale).predicate(Buyer).subject(bambam).object(P00001)"),
                           "{}".format([[b'key', b'Buyer:P00001:bambam', b'type', b'H', b'score', b'1.000000'], [b'key', b'Buyer:bambam:P00001', b'type', b'H', b'score', b'1.000000']]))
    
    succes_tally += match("add purchase 1.1",data.execute_command("RXQUERY", "G:predicate(Item).subject(Amstel).object(P00001)"),
                           "{}".format([[b'key', b'Item:Amstel:P00001', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:P00001:Amstel', b'type', b'H', b'score', b'1.000000']]))
    
    succes_tally += match("add purchase 1.2",data.execute_command("RXQUERY", "G:predicate(Item).subject(BottledWater).object(P00001)"), 
                          "{}".format([[b'key', b'Item:BottledWater:P00001', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:P00001:BottledWater', b'type', b'H', b'score', b'1.000000']]))
        
    succes_tally += match("add purchase 2.0",data.execute_command("RXQUERY", "G:AddV(P00002,Sale).predicate(Buyer).subject(fred).object(P00002)"), 
                          "{}".format([[b'key', b'Buyer:P00002:fred', b'type', b'H', b'score', b'1.000000'], [b'key', b'Buyer:fred:P00002', b'type', b'H', b'score', b'1.000000']]))

    succes_tally += match("add purchase 2.1",data.execute_command("RXQUERY", "G:predicate(Item).subject(Amstel).object(P00002)"), 
                          "{}".format([[b'key', b'Item:Amstel:P00002', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:P00002:Amstel', b'type', b'H', b'score', b'1.000000']]))
    
    succes_tally += match("add purchase 2.2",data.execute_command("RXQUERY", "G:predicate(Item).subject(RedBull).object(P00002)"), 
                          "{}".format([[b'key', b'Item:P00002:RedBull', b'type', b'H', b'score', b'1.000000'], [b'key', b'Item:RedBull:P00002', b'type', b'H', b'score', b'1.000000']]))
    
    succes_tally += match("backup db","{}".format(data.execute_command("BGSAVE")), "True")
    bambam = data.hgetall("bambam")
    succes_tally += match("check bambam", bambam, "{}".format( {b'type': b'MALE', b'age': b'9'}))
    fred = data.hgetall("fred")
    succes_tally += match("check fred", fred, "{}".format( {b'type': b'MALE', b'age': b'47'}))
    succes_tally += match("wait on indexing", data.execute_command("rxIndex wait").decode(), 'OK')

    succes_tally += match("validate age check", data.execute_command("RXGET", "g:v().has(verification,age)"), 
                          "{}".format([[b'key', b'bambam', b'score', b'1.000000', b'value', [b'type', b'MALE', b'age', b'9', b'verification', b'age']]]))

    succes_tally += match("rule list", data.execute_command("RULE.LIST"), 
                          "{}".format([[b'name', b'age_restriction', b'rule', b' type sale , HAS workset AS Item IN type beer , HAS Item OUT workset USE & . . . . Buyer IN age 21 , LT verification age , PROPERTY Buyer OUT workset USE & . . . . . breach AS rpush kettle2 , @graph , REDIS DROP RESET breach USE . . . . . .', b'no of applies', 42, b'no of skips', 66, b'no of hits', 3, b'no of misses', 39]]))


    succes_tally += match("check queue", data.lrange("kettle2", 0, 1000), 
                          "{}".format([b'{"entities":[{"iri":"P00000", "entity":"vertice","type": "Sale","verification": "age"},{"iri":"Amstel", "entity":"vertice","type": "beer"},{"iri":"bambam", "entity":"vertice","type": "MALE","age": "9","verification": "age"},{"iri":"BottledWater", "entity":"vertice","type": "nonalcoholic"}]", triplets":[{"predicate":"P00000", "subject":"Item:P00000:Amstel", "object":"Buyer:bambam:P00000", "depth":0},{"predicate":"Item:P00000:Amstel", "subject":"P00000", "object":"Amstel", "depth":1},{"predicate":"Amstel", "subject":"Item:Amstel:P00001", "object":"Item:P00000:Amstel", "depth":2},{"predicate":"Buyer:bambam:P00000", "subject":"bambam", "object":"P00000", "depth":1},{"predicate":"bambam", "subject":"Buyer:bambam:P00000", "object":"son_of:barney:bambam", "depth":2},{"predicate":"Item:BottledWater:P00000", "subject":"BottledWater", "object":"P00000", "depth":1},{"predicate":"BottledWater", "subject":"Item:BottledWater:P00001", "object":"Item:P00000:BottledWater", "depth":2}]}', b'{"entities":[{"iri":"P00001", "entity":"vertice","type": "Sale","verification": "age"},{"iri":"Amstel", "entity":"vertice","type": "beer"},{"iri":"bambam", "entity":"vertice","type": "MALE","age": "9","verification": "age"},{"iri":"BottledWater", "entity":"vertice","type": "nonalcoholic"}]", triplets":[{"predicate":"P00001", "subject":"Item:P00001:Amstel", "object":"Item:Amstel:P00001", "depth":0},{"predicate":"Item:P00001:Amstel", "subject":"P00001", "object":"Amstel", "depth":1},{"predicate":"Amstel", "subject":"Item:Amstel:P00001", "object":"Item:P00000:Amstel", "depth":2},{"predicate":"Buyer:bambam:P00001", "subject":"bambam", "object":"P00001", "depth":1},{"predicate":"bambam", "subject":"Buyer:bambam:P00000", "object":"son_of:barney:bambam", "depth":2},{"predicate":"Item:BottledWater:P00001", "subject":"BottledWater", "object":"P00001", "depth":1},{"predicate":"BottledWater", "subject":"Item:BottledWater:P00001", "object":"Item:P00000:BottledWater", "depth":2}]}', b'{"entities":[{"iri":"P00002", "entity":"vertice","type": "Sale","verification": "age"},{"iri":"fred", "entity":"vertice","type": "MALE","age": "47"},{"iri":"Amstel", "entity":"vertice","type": "beer"},{"iri":"RedBull", "entity":"vertice","type": "sports"}]", triplets":[{"predicate":"P00002", "subject":"Buyer:P00002:fred", "object":"Item:Amstel:P00002", "depth":0},{"predicate":"Buyer:P00002:fred", "subject":"P00002", "object":"fred", "depth":1},{"predicate":"fred", "subject":"Buyer:fred:P00002", "object":"Buyer:P00002:fred", "depth":2},{"predicate":"Item:Amstel:P00002", "subject":"Amstel", "object":"P00002", "depth":1},{"predicate":"Amstel", "subject":"Item:Amstel:P00001", "object":"Item:P00000:Amstel", "depth":2},{"predicate":"Item:RedBull:P00002", "subject":"RedBull", "object":"P00002", "depth":1},{"predicate":"RedBull", "subject":"Item:RedBull:P00002", "object":"Item:P00002:RedBull", "depth":2}]}']))

    if(succes_tally != get_match_calls()): 
        raise AssertionError( "FAILED: SD_103_Business_Rules_and_or_Triggers, {} of {} steps failed".format(get_match_calls() - succes_tally, get_match_calls()))
        
   