import time
import redis
from match import Matcher

          
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
    matcher = Matcher("SD_103_Business_Rules_Data_Augmentation_Verify_Traverse_Directions")
    matcher.by_hashed("KEYS", data.execute_command("keys", "*"), [])
    matcher.by_hashed("KEYS", index.execute_command("keys", "*"), [])

    matcher.by_strings("rule list", data.execute_command("RULE.LIST"),                           b'No rules defined')

    matcher.by_strings("get dataset",data.execute_command("g.wget", "https://roxoft.dev/assets/dataset2.txt").decode(), 
                          'dataset2.txt')
    matcher.by_strings("add rule linkFather",data.execute_command("RULE.SET", "linkFather", "(haslabel(father).addE(@,father,father_of))" ), b'Rule for: linkFather Expression: Expression appears to be incorrect!')
    matcher.by_strings("add rule linkMother",data.execute_command("RULE.SET", "linkMother", "(haslabel(mother).addE(@,mother,mother_of))" ), b'Rule for: linkMother Expression: Expression appears to be incorrect!')
    matcher.by_strings("add rule linkBrother",data.execute_command("RULE.SET", "linkBrother", "(haslabel(brother).addE(@brother,has_brother,brother_of))" ), b'Rule for: linkBrother Expression: Expression appears to be incorrect!')
    matcher.by_strings("add rule linkSister",data.execute_command("RULE.SET", "linkSister", "(haslabel(sister).addE(@sister,has_sister,sister_of))" ), b'Rule for: linkSister Expression: Expression appears to be incorrect!')
    matcher.by_strings("add rule linkSpouse",data.execute_command("RULE.SET", "linkSpouse", "(haslabel(spouse).addE(@,spouse))" ), b'Rule for: linkSpouse Expression: Expression appears to be incorrect!')
    matcher.by_strings("add rule linkFriend",data.execute_command("RULE.SET", "linkFriend", "(haslabel(friend).addE(@,friend))" ), b'Rule for: linkFriend Expression: Expression appears to be incorrect!')
    matcher.by_strings("add rule linkOwner",data.execute_command("RULE.SET", "linkOwner", "(haslabel(owner).addE(@owner,owned_by,owner_of))" ), b'Rule for: linkOwner Expression: Expression appears to be incorrect!')
    matcher.by_strings("add rule linkBoss",data.execute_command("RULE.SET", "linkBoss", "(haslabel(boss).addE(@boss,has_boss,boss_of))" ), b'Rule for: linkBoss Expression: Expression appears to be incorrect!')
    matcher.by_strings("LOAD TEXT FILE",data.execute_command("LOAD.TEXT", "CRLF", "TAB_SEPARATED", "FROM", "dataset2.txt", "as_hash"), b'OK')    
    time.sleep(5)
    matcher.by_strings("wait on indexing", data.execute_command("rxIndex wait"), b'OK')



    matcher.by_hashed("verify out (is)mother == Mothers", data.execute_command("rxquery","g:v(person).out(mother)","ranked"), 
                          [[b'key', b'betty', b'type', b'H', b'score', b'1.000000'], [b'key', b'flo', b'type', b'H', b'score', b'1.000000'], [b'key', b'pearl', b'type', b'H', b'score', b'1.000000'], [b'key', b'pebbles', b'type', b'H', b'score', b'1.000000'], [b'key', b'wilma', b'type', b'H', b'score', b'1.000000']])

    matcher.by_hashed("verify in (has)mother == Children", data.execute_command("rxquery","g:v(person).in(mother)","ranked"), 
                          [[b'key', b'bammbamm', b'type', b'H', b'score', b'1.000000'], [b'key', b'barney', b'type', b'H', b'score', b'1.000000'], [b'key', b'chip', b'type', b'H', b'score', b'1.000000'], [b'key', b'pebbles', b'type', b'H', b'score', b'1.000000'], [b'key', b'roxy', b'type', b'H', b'score', b'1.000000'], [b'key', b'wilma', b'type', b'H', b'score', b'1.000000']])

    matcher.by_hashed("verify in children who ARE MOTHERS", data.execute_command("rxquery","g:((v().hasout(mother)) (v.hasin(mother)) &).outT(mother)","ranked"), 
                          [[b'subject', b'wilma', b'length', b'1', b'object', b'wilma', b'path', [[b'wilma', b'1']]], [b'subject', b'pebbles', b'length', b'1', b'object', b'pebbles', b'path', [[b'pebbles', b'1']]], [b'subject', b'pebbles', b'length', b'1', b'object', b'pebbles', b'path', [[b'pebbles', b'1']]], [b'subject', b'pebbles', b'length', b'1', b'object', b'pebbles', b'path', [[b'pebbles', b'1']]]])

    matcher.finalize()
        