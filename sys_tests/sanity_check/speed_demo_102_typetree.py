import redis
from match import Matcher
          
#
# This test verifies the VERTEX and EDGE type tree features#

def SD_102_Traversing_TypeTree(cluster_id, controller, data, index):
    matcher = Matcher("SD_101_Non_Intrusive_Indexing_And_Full_Text_Queries")

    matcher.by_sets("KEYS", data.execute_command("keys", "*"), [])
    matcher.by_sets("KEYS", index.execute_command("keys", "*"), [])

    matcher.by_strings("rule list", data.execute_command("RULE.LIST"), b'No rules defined')

    matcher.by_strings("get dataset",data.execute_command("g.wget", "https://roxoft.dev/assets/products.txt"), b'products.txt')
    
    matcher.by_strings("LOAD TEXT FILE",data.execute_command("LOAD.TEXT", "CRLF", "TAB_SEPARATED", "FROM", "products.txt", "as_hash"), b'OK')
    
    matcher.by_strings("type tree whiskies", data.execute_command("rxtypetree whiskies whiskey bourbon whisky "), b'`whiskies')
    
    matcher.by_strings("type tree alcoholic", data.execute_command("rxtypetree alcoholic whiskies wine beer liqueur "), b'`alcoholic')

    matcher.by_strings("type tree all beverages", data.execute_command("rxtypetree beverages alcoholic water pop energy"), b'`beverages')
    
    matcher.by_strings("wait on indexing", data.execute_command("rxIndex wait"), b'OK')



    matcher.by_hashed("verify whiskies", data.execute_command("rxquery","g:v(whiskies)","ranked"), 
                          [[b'key', b'w1', b'type', b'H', b'score', b'1.000000'], [b'key', b'w10', b'type', b'H', b'score', b'1.000000'], [b'key', b'w11', b'type', b'H', b'score', b'1.000000'], [b'key', b'w12', b'type', b'H', b'score', b'1.000000'], [b'key', b'w13', b'type', b'H', b'score', b'1.000000'], [b'key', b'w14', b'type', b'H', b'score', b'1.000000'], [b'key', b'w2', b'type', b'H', b'score', b'1.000000'], [b'key', b'w3', b'type', b'H', b'score', b'1.000000'], [b'key', b'w4', b'type', b'H', b'score', b'1.000000'], [b'key', b'w5', b'type', b'H', b'score', b'1.000000'], [b'key', b'w6', b'type', b'H', b'score', b'1.000000'], [b'key', b'w7', b'type', b'H', b'score', b'1.000000'], [b'key', b'w8', b'type', b'H', b'score', b'1.000000'], [b'key', b'w9', b'type', b'H', b'score', b'1.000000']])

    matcher.by_hashed("verify alcoholic", data.execute_command("rxquery","g:v(alcoholic)","ranked"), 
                          [[b'key', b'beer1', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer2', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer3', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer4', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer5', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer6', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer7', b'type', b'H', b'score', b'1.000000'], [b'key', b'liqor1', b'type', b'H', b'score', b'1.000000'], [b'key', b'w1', b'type', b'H', b'score', b'1.000000'], [b'key', b'w10', b'type', b'H', b'score', b'1.000000'], [b'key', b'w11', b'type', b'H', b'score', b'1.000000'], [b'key', b'w12', b'type', b'H', b'score', b'1.000000'], [b'key', b'w13', b'type', b'H', b'score', b'1.000000'], [b'key', b'w14', b'type', b'H', b'score', b'1.000000'], [b'key', b'w2', b'type', b'H', b'score', b'1.000000'], [b'key', b'w3', b'type', b'H', b'score', b'1.000000'], [b'key', b'w4', b'type', b'H', b'score', b'1.000000'], [b'key', b'w5', b'type', b'H', b'score', b'1.000000'], [b'key', b'w6', b'type', b'H', b'score', b'1.000000'], [b'key', b'w7', b'type', b'H', b'score', b'1.000000'], [b'key', b'w8', b'type', b'H', b'score', b'1.000000'], [b'key', b'w9', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine1', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine2', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine3', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine4', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine5', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine6', b'type', b'H', b'score', b'1.000000']])

    matcher.by_hashed("verify beverages", data.execute_command("rxquery","g:v(beverages)","ranked"), 
                          [[b'key', b'beer1', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer2', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer3', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer4', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer5', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer6', b'type', b'H', b'score', b'1.000000'], [b'key', b'beer7', b'type', b'H', b'score', b'1.000000'], [b'key', b'energy1', b'type', b'H', b'score', b'1.000000'], [b'key', b'energy2', b'type', b'H', b'score', b'1.000000'], [b'key', b'energy3', b'type', b'H', b'score', b'1.000000'], [b'key', b'energy4', b'type', b'H', b'score', b'1.000000'], [b'key', b'energy5', b'type', b'H', b'score', b'1.000000'], [b'key', b'energy6', b'type', b'H', b'score', b'1.000000'], [b'key', b'energy7', b'type', b'H', b'score', b'1.000000'], [b'key', b'energy8', b'type', b'H', b'score', b'1.000000'], [b'key', b'liqor1', b'type', b'H', b'score', b'1.000000'], [b'key', b'soft1', b'type', b'H', b'score', b'1.000000'], [b'key', b'soft2', b'type', b'H', b'score', b'1.000000'], [b'key', b'soft3', b'type', b'H', b'score', b'1.000000'], [b'key', b'soft4', b'type', b'H', b'score', b'1.000000'], [b'key', b'soft5', b'type', b'H', b'score', b'1.000000'], [b'key', b'w1', b'type', b'H', b'score', b'1.000000'], [b'key', b'w10', b'type', b'H', b'score', b'1.000000'], [b'key', b'w11', b'type', b'H', b'score', b'1.000000'], [b'key', b'w12', b'type', b'H', b'score', b'1.000000'], [b'key', b'w13', b'type', b'H', b'score', b'1.000000'], [b'key', b'w14', b'type', b'H', b'score', b'1.000000'], [b'key', b'w2', b'type', b'H', b'score', b'1.000000'], [b'key', b'w3', b'type', b'H', b'score', b'1.000000'], [b'key', b'w4', b'type', b'H', b'score', b'1.000000'], [b'key', b'w5', b'type', b'H', b'score', b'1.000000'], [b'key', b'w6', b'type', b'H', b'score', b'1.000000'], [b'key', b'w7', b'type', b'H', b'score', b'1.000000'], [b'key', b'w8', b'type', b'H', b'score', b'1.000000'], [b'key', b'w9', b'type', b'H', b'score', b'1.000000'], [b'key', b'water1', b'type', b'H', b'score', b'1.000000'], [b'key', b'water2', b'type', b'H', b'score', b'1.000000'], [b'key', b'water3', b'type', b'H', b'score', b'1.000000'], [b'key', b'water4', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine1', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine2', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine3', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine4', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine5', b'type', b'H', b'score', b'1.000000'], [b'key', b'wine6', b'type', b'H', b'score', b'1.000000']])

    matcher.finalize()