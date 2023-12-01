import redis
from match import Matcher, HeaderOnly

def SD_102_Traversing_a_Redis_Database_Redis_as_a_GraphDB(cluster_id, controller, data, index):
    matcher = Matcher("SD_102_Traversing_a_Redis_Database_Redis_as_a_GraphDB")

    matcher.by_strings("get test data", data.execute_command("g.wget", "https://roxoft.dev/assets/bedrock1.txt"), b'bedrock1.txt')
    matcher.by_strings("load test data", data.execute_command("g.set",  "FILE", "bedrock1.txt"), b'OK')
    matcher.by_strings("wait indexing completed", data.execute_command("rxIndex wait"), b'OK')
    matcher.by_strings("Fred as hash", data.hgetall("fred"), 
                        {b'age': b'40', b'type': b'male', b'name': b'Fred Flintstone', b'image': b"%3cimg src='Stories/assets/fred.jpg' width='50px'  max-width='50px'   max-heigth='50px' /%3e"})

    matcher.by_hashed("All Males", data.execute_command("rxquery", "g:v(male)", "ranked"), 
                        [b'key', b'chip', b'type', b'H', b'score', b'1.000000', b'key', b'fred', b'type', b'H', b'score', b'1.000000', b'key', b'bambam', b'type', b'H', b'score', b'1.000000', b'key', b'barney', b'type', b'H', b'score', b'1.000000', b'key', b'mrslate', b'type', b'H', b'score', b'1.000000'])
    
    matcher.by_hashed("Fred and Dino", data.execute_command("rxquery", "g:v().out(owner)", "ranked"), 
                        [b'key', b'bqgc', b'type', b'H', b'score', b'1.000000', b'key', b'dino', b'type', b'H', b'score', b'1.000000', b'key', b'fred', b'type', b'H', b'score', b'1.000000', b'key', b'mrslate', b'type', b'H', b'score', b'1.000000'])
    
    matcher.by_sets("Fred and Flo", HeaderOnly(data.execute_command("rxquery", "g:v().match(fred,flo)", "ranked")), 
                        [[b'subject', b'fred', b'length', b'2', b'object', b'flo']])
    
    matcher.by_hashed("add Frankenstones",data.execute_command("rxquery", "g:addv(hidea,female).property(name,'Hidea Frankenstone').property(image,'hidea.jpg').addv(frank,male).property(name,'Frank Frankenstone').property(image,'frank.jpg').addv(freaky,male).property(name,'Freaky Frankenstone').property(image,'freaky.jpg').addv(atrocia,female).property(name,'Atrocia Frankenstone').property(image,'atrocia.jpg')"), 
                        [b'key', b'atrocia', b'type', b'H', b'score', b'1.000000'])

    matcher.by_hashed("add Frankenstone Family ties",data.execute_command("rxquery", "g:predicate(father,son).subject(frank).object(freaky).predicate(mother,son).subject(frank).object(freaky).predicate(father,daughter).subject(frank).object(atrocia).predicate(mother,daughter).subject(hidea).object(atrocia)"), 
                         [b'key', b'daughter:atrocia:hidea', b'type', b'H', b'score', b'1.000000', b'key', b'daughter:daughter:atrocia:frank:hidea', b'type', b'H', b'score', b'1.000000', b'key', b'daughter:daughter:mother:frank:father:frank:freaky:frank:hidea', b'type', b'H', b'score', b'1.000000', b'key', b'daughter:daughter:mother:frank:freaky:frank:hidea', b'type', b'H', b'score', b'1.000000', b'key', b'daughter:daughter:mother:frank:son:freaky:frank:frank:hidea', b'type', b'H', b'score', b'1.000000', b'key', b'daughter:daughter:son:father:frank:freaky:frank:frank:hidea', b'type', b'H', b'score', b'1.000000', b'key', b'daughter:daughter:son:freaky:frank:frank:hidea', b'type', b'H', b'score', b'1.000000', b'key', b'daughter:daughter:son:son:freaky:frank:frank:frank:hidea', b'type', b'H', b'score', b'1.000000', b'key', b'daughter:father:frank:atrocia:hidea', b'type', b'H', b'score', b'1.000000', b'key', b'daughter:father:frank:mother:frank:father:frank:freaky:hidea', b'type', b'H', b'score', b'1.000000', b'key', b'daughter:father:frank:mother:frank:freaky:hidea', b'type', b'H', b'score', b'1.000000', b'key', b'daughter:father:frank:mother:frank:son:freaky:frank:hidea', b'type', b'H', b'score', b'1.000000', b'key', b'daughter:father:frank:son:father:frank:freaky:frank:hidea', b'type', b'H', b'score', b'1.000000', b'key', b'daughter:father:frank:son:freaky:frank:hidea', b'type', b'H', b'score', b'1.000000', b'key', b'daughter:father:frank:son:son:freaky:frank:frank:hidea', b'type', b'H', b'score', b'1.000000', b'key', b'mother:hidea:atrocia', b'type', b'H', b'score', b'1.000000', b'key', b'mother:hidea:daughter:atrocia:frank', b'type', b'H', b'score', b'1.000000', b'key', b'mother:hidea:daughter:mother:frank:father:frank:freaky:frank', b'type', b'H', b'score', b'1.000000', b'key', b'mother:hidea:daughter:mother:frank:freaky:frank', b'type', b'H', b'score', b'1.000000', b'key', b'mother:hidea:daughter:mother:frank:son:freaky:frank:frank', b'type', b'H', b'score', b'1.000000', b'key', b'mother:hidea:daughter:son:father:frank:freaky:frank:frank', b'type', b'H', b'score', b'1.000000', b'key', b'mother:hidea:daughter:son:freaky:frank:frank', b'type', b'H', b'score', b'1.000000', b'key', b'mother:hidea:daughter:son:son:freaky:frank:frank:frank', b'type', b'H', b'score', b'1.000000', b'key', b'mother:hidea:father:frank:atrocia', b'type', b'H', b'score', b'1.000000', b'key', b'mother:hidea:father:frank:mother:frank:father:frank:freaky', b'type', b'H', b'score', b'1.000000', b'key', b'mother:hidea:father:frank:mother:frank:freaky', b'type', b'H', b'score', b'1.000000', b'key', b'mother:hidea:father:frank:mother:frank:son:freaky:frank', b'type', b'H', b'score', b'1.000000', b'key', b'mother:hidea:father:frank:son:father:frank:freaky:frank', b'type', b'H', b'score', b'1.000000', b'key', b'mother:hidea:father:frank:son:freaky:frank', b'type', b'H', b'score', b'1.000000', b'key', b'mother:hidea:father:frank:son:son:freaky:frank:frank', b'type', b'H', b'score', b'1.000000'])

    matcher.by_hashed("match(freaky,atrocia)",HeaderOnly(data.execute_command("rxquery", "g:v().match(freaky,atrocia)", "ranked")), 
                         [[b'subject', b'freaky', b'length', b'2', b'object', b'atrocia']])

    matcher.finalize()      