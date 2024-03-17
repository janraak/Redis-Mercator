import redis
from match import Matcher, HeaderOnly, clone_client

# This test will verify that the rxMercator commands will execute correctly asynchronously.
#
# 1) Two data sets are loaded.
#    a) Major World cities.
#    b) Some routes between thos cities.
#       i) Multiple routes between two cities may exist for different means of transportaion.
#       ii) Each route contains:
#           I) The estimated travel time (Rome2Rio)
#           II) The Distance in Kilometers (Rome2Rio)
#           I) The travel costs (Rome2Rio)
#           I) The number of trees to be replenished (Some other website). Carbondioxide emission.
#    c) Tests the load text command.
# 2) Two rules (added before loading):
#    a) Split the imported city data into a city and an administrative region and edge between the two.        
#    b) Split the administrative region data into an land (country) and edge between the two.        
#    c) Tests the cloveTriplet step function.
# 3) Create routes between the cities using the imported route hash keys.
#    a) Create the edges between the 'from' city and the 'to' city.
#    b) Copy the weight (dimension) attributes to the edge.eval.EncodingWarning
#    c) Test of the triplet stepfuncion with attribute copy list.
# 4) Execute some set base queries.
# 5) Find routes between cities using various weight.
#    a) Test the match step function.
#    b) Test the minimize step function.
#    c) Test the exclude step function.

# def SD_204_Async_Test(cluster_id, controller, data, index):
#     matcher = Matcher("SD_204_Async_Test")
#     # dataset = "worldcities.csv" 
#     dataset = 'worldcities_small.csv'
#     scenario = [
#         {"step": "add rule clove city in city and admin region", "node": data, "command": "RULE.SET cloveAdminRegion FINAL IF ((has(type,town)).haslabel(country)) THEN (cloveTriplet( @admin_name, admin_region, @key, (admin_for, town_of), country, iso2, iso3))", "expects": b'Rule for: cloveAdminRegion Expression: Expression appears to be incorrect!'},
#         {"step": "add rule clove admin region in land and admin region", "node": data, "command": "RULE.SET cloveLand FINAL IF ([has(type,admin_region)].haslabel(country)) THEN (cloveTriplet( @country, land, @key, (country_for, admin_in), iso2, iso3))", "expects": b'Rule for: cloveLand Expression: Expression appears to be incorrect!'},
#         {"step": "statistics", "node": data, "command": "rule.list", "expects": b'?'},
#         {"step": "get test data - routes", "node": data, "command": "g.wget https://roxoft.dev/assets/EU_routes.csv", "expects": b'EU_routes.csv'},
#         {"step": "get test data - places", "node": data, "command": "g.wget https://roxoft.dev/assets/" + dataset, "expects": bytes(dataset, 'utf-8')},
#         {"step": "check keyspace 1", "node": data, "command": "info keyspace", "expects": b'# Keyspace\r\n'},
#         {"step": "load test data - places", "node": data, "command": "load.text crlf comma_separated from "+dataset+" as_hash key city_ascii type town", "expects": b'OK'},
#         {"step": "statistics", "node": data, "command": "rxindex", "expects": b'?'},
#         {"step": "check keyspace 3", "node": data, "command": "info keyspace", "expects": b'# Keyspace\r\ndb0:keys=41350,expires=0,avg_ttl=0\r\n'},
#         {"step": "load test data - routes", "node": data, "command": "load.text crlf comma_separated from EU_routes.csv as_hash key Route type leg", "expects": b'OK'},
#         {"step": "check keyspace 2", "node": data, "command": "info keyspace", "expects": b'# Keyspace\r\ndb0:keys=209,expires=0,avg_ttl=0\r\n'},
#         {"step": "setup routes", "node": data, "command": "g v(leg).triplet(@From,@By,@By,@To,(Time,Km,Euro,Trees))", "expects": [], "strip":[b'key', b'score',b'value']},
#         {"step": "check keyspace 4", "node": data, "command": "info keyspace", "expects": b'# Keyspace\r\ndb0:keys=42233,expires=0,avg_ttl=0\r\n'},
#         {"step": "statistics", "node": data, "command": "rxindex", "expects": b'?'},
#         {"step": "statistics", "node": data, "command": "rxindex wait", "expects": b'?'},
#         {"parallel": {"name": "Set Queries", "repeats": 10, "steps": [        
#         {"step": "query some cities", "node": data, "command": "Q ((city_ascii == amsterdam) (city_ascii == seattle) (city_ascii == madrid)) & town", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
#         {"step": "query some cities", "node": data, "command": "Q (Amsterdam | Rome | Madrid | Paris | Kyiv | Warsaw | Ankara)", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
#         {"step": "Traverse Amsterdam", "node": data, "command": "g v(Amsterdam).in(admin_for).out(admin_in) ", "expects": []},
#         {"step": "Traverse France", "node": data, "command": "g v(France).in(admin_in).out(admin_for)", "expects": []},
#         {"step": "Match Amsterdam->Rome - default", "node": data, "command": "g match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'3', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'fly:Amsterdam:Dublin', b'0.5'], [b'Dublin', b'0.5'], [b'carFerry:Dublin:Paris', b'0.5'], [b'Paris', b'0.5'], [b'train:Paris:Rome', b'0.5'], [b'Rome', b'0.5']]]], "strip": [b'length', b'path']},
#         {"step": "Match Amsterdam->Rome - minimize trees", "node": data, "command": "g minimize(Trees).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'665', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip": [b'length', b'path']},
#         {"step": "Match Amsterdam->Rome - default - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'3', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'fly:Amsterdam:Dublin', b'0.5'], [b'Dublin', b'0.5'], [b'carFerry:Dublin:Paris', b'0.5'], [b'Paris', b'0.5'], [b'train:Paris:Rome', b'0.5'], [b'Rome', b'0.5']]]], "strip":[b'path']},
#         {"step": "Match Amsterdam->Rome - minimize km - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Km).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'1119', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
#         {"step": "Match Amsterdam->Rome - minimize euro - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Euro).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'180.5', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
#         {"step": "Match Amsterdam->Rome - minimize trees - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Trees).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'665', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
#         {"step": "Match Amsterdam->Rome - minimize time - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Time).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'11', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
#         {"step": "Match Amsterdam,Rome, Madrid,Paris,Kyiv, Warsaw,Ankara - minimize trees - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Trees).match(Amsterdam,Rome, Madrid,Paris,Kyiv, Warsaw,Ankara) ", "expects": [[b'subject', b'Amsterdam->Ankara->Amsterdam->Rome->Rome->Madrid->Madrid->Paris->Paris->Kyiv->Kyiv->Warsaw->Warsaw->Ankara', b'length', b'5816']], "strip":[b'path', b'objects']},
#         {"step": "statistics", "node": data, "command": "rxindex", "expects": b'?'},
#             ]}, "clients": 100
#          },
#     ]
#     matcher.play(scenario)

def SD_210_Async_Endurance_Test(cluster_id, controller, data, index):
    matcher = Matcher("SD_210_Async_Endurance_Test")
    # dataset = "worldcities.csv" 
    # dataset = "worldcities_only_capitals.csv" 
    dataset = 'worldcities_small.csv'

    scenario = [
        {"parallel": {"name": "Load DB", "repeats": 1, "steps": [        
        {"step": "increase maxmemory", "node": data, "command": "config set maxmemory 48gb", "expects": b'OK'},
        {"step": "add rule clove city in city and admin region", "node": data, "command": "RULE.SET cloveAdminRegion FINAL IF (has(type,town).haslabel(country)) THEN (cloveTriplet( @admin_name, admin_region, @key, (admin_for, town_of), country, iso2, iso3))", "expects": b'Rule for: cloveAdminRegion Expression: Expression appears to be incorrect!'},
        {"step": "add rule clove admin region in land and admin region", "node": data, "command": "RULE.SET cloveLand FINAL IF (has(type,admin_region).haslabel(country)) THEN (cloveTriplet( @country, land, @key, (country_for, admin_in), iso2, iso3))", "expects": b'Rule for: cloveLand Expression: Expression appears to be incorrect!'},
        {"step": "get test data - routes", "node": data, "command": "g.wget https://roxoft.dev/assets/EU_routes.csv", "expects": b'EU_routes.csv'},
        {"step": "get test data - places", "node": data, "command": "g.wget https://roxoft.dev/assets/" + dataset, "expects": bytes(dataset, 'utf-8')},
        {"step": "check keyspace 1", "node": data, "command": "info keyspace", "expects": b'# Keyspace\r\n'},
        {"step": "load test data - routes", "node": data, "command": "load.text crlf comma_separated from EU_routes.csv as_hash key Route type leg", "expects": b'OK'},
        {"step": "check keyspace 2", "node": data, "command": "info keyspace", "expects": b'# Keyspace\r\ndb0:keys=209,expires=0,avg_ttl=0\r\n'},
        {"step": "load test data - places", "node": data, "command": f"load.text crlf comma_separated from {dataset} as_hash key city_ascii type town", "expects": b'OK'},
        {"step": "statistics", "node": data, "command": "rxindex", "expects": b'?'},
        {"step": "check keyspace 3", "node": data, "command": "info keyspace", "expects": b'# Keyspace\r\ndb0:keys=41350,expires=0,avg_ttl=0\r\n'},
        {"step": "setup routes", "node": data, "command": "g v(leg).triplet(@From,@By,@By,@To,(Time,Km,Euro,Trees))", "expects": [], "strip":[b'key', b'score',b'value']},
        # {"step": "setup routes", "node": data, "command": "g v(leg).triplet(@From,@By,@By,@To,(Time,Km,Euro,Trees)).reset().v(leg).drop().v(leg) ", "expects": [], "strip":[b'key', b'score',b'value']},
        {"step": "check keyspace 4", "node": data, "command": "info keyspace", "expects": b'# Keyspace\r\ndb0:keys=42233,expires=0,avg_ttl=0\r\n'},
        {"step": "statistics", "node": data, "command": "rxindex wait", "expects": b'?'},
        {"step": "statistics", "node": data, "command": "rxindex", "expects": b'?'},
        {"step": "query some cities", "node": data, "command": "Q ((city_ascii == amsterdam) (city_ascii == seattle) (city_ascii == madrid)) & town", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
        {"step": "query some cities", "node": data, "command": "Q (Amsterdam Rome Madrid Paris Kyiv Warsaw Ankara)", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
        {"step": "Match Amsterdam->Rome - default", "node": data, "command": "g match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'3', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'fly:Amsterdam:Dublin', b'0.5'], [b'Dublin', b'0.5'], [b'carFerry:Dublin:Paris', b'0.5'], [b'Paris', b'0.5'], [b'train:Paris:Rome', b'0.5'], [b'Rome', b'0.5']]]], "strip": [b'length', b'path']},
        {"step": "Match Amsterdam->Rome - minimize trees", "node": data, "command": "g minimize(Trees).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'665', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip": [b'length', b'path']},
        {"step": "Match Amsterdam->Rome - default - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'3', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'fly:Amsterdam:Dublin', b'0.5'], [b'Dublin', b'0.5'], [b'carFerry:Dublin:Paris', b'0.5'], [b'Paris', b'0.5'], [b'train:Paris:Rome', b'0.5'], [b'Rome', b'0.5']]]], "strip":[b'path']},
        {"step": "Match Amsterdam->Rome - minimize km - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Km).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'1119', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
        {"step": "Match Amsterdam->Rome - minimize euro - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Euro).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'180.5', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
        {"step": "Match Amsterdam->Rome - minimize trees - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Trees).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'665', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
        {"step": "Match Amsterdam->Rome - minimize time - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Time).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'11', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
        {"step": "Match Amsterdam,Rome, Madrid,Paris,Kyiv, Warsaw,Ankara - minimize trees - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Trees).match(Amsterdam,Rome, Madrid,Paris,Kyiv, Warsaw,Ankara) ", "expects": [[b'subject', b'Amsterdam', b'length', b'665', b'object', b'Rome']], "strip":[b'path', b'objects']},
        {"step": "statistics", "node": data, "command": "rxindex", "expects": b'?'},
            ]}, "clients": 100
         },
        {"parallel": {"name": "Set Queries", "repeats": 5000, "steps": [        
            # {"step": "query some cities", "node": data, "command": "Q ((city_ascii == amsterdam) (city_ascii == seattle) (city_ascii == madrid)) & town", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
            {"step": "query some cities", "node": data, "command": "Q (Amsterdam Rome Madrid Paris Kyiv Warsaw Ankara)", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
            # {"step": "status", "node": data, "command": "info clients", "expects":b'*blocked*'},{"step": "status", "node": index, "command": "info clients", "expects":b'*blocked: 0*'},
            ]}, "clients": 100
         },
        {"parallel": {"name": "Updates", "repeats": 5000, "steps": [        
            {"step": "query some cities", "node": data, "command": "g v(amsterdam).add (Amsterdam Rome Madrid Paris Kyiv Warsaw Ankara)", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
            ]}, "clients": 100
         },
        {"parallel": {"name": "Set Queries", "repeats": 5000, "steps": [        
            {"step": "Traverse Amsterdam", "node": data, "command": "g v(Amsterdam).in(admin_for).out(admin_in) ", "expects": []},
            ]}, "clients": 100
         },
        {"parallel": {"name": "Set Queries", "repeats": 5000, "steps": [        
            {"step": "Traverse France", "node": data, "command": "g v(France).in(admin_in).out(admin_for)", "expects": []},
            ]}, "clients": 100
         },
        {"parallel": {"name": "Set Queries", "repeats": 5000, "steps": [        
            # {"step": "query some cities", "node": data, "command": "Q ((city_ascii == amsterdam) (city_ascii == seattle) (city_ascii == madrid)) & town", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
            {"step": "query some cities", "node": data, "command": "Q  town && (Tallinn Dublin)", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
            # {"step": "status", "node": data, "command": "info clients", "expects":b'*blocked*'},{"step": "status", "node": index, "command": "info clients", "expects":b'*blocked: 0*'},
            ]}, "clients": 100
         },
#         {"parallel": {"name": "Check blocked", "repeats": 5000, "pace": 6, "steps": [        
#             # {"step": "status", "node": data, "command": "info clients", "expects":b'*blocked*'},{"step": "status", "node": index, "command": "info clients", "expects":b'*blocked: 0*'},
#                         # {"step": "status", "node": data, "command": "info clients", "expects":b'*blocked*'},{"step": "status", "node": index, "command": "info clients", "expects":b'*blocked: 0*'},
# ]}, "clients": 100,
#          },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - default", "node": data, "command": "g match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'3', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'fly:Amsterdam:Dublin', b'0.5'], [b'Dublin', b'0.5'], [b'carFerry:Dublin:Paris', b'0.5'], [b'Paris', b'0.5'], [b'train:Paris:Rome', b'0.5'], [b'Rome', b'0.5']]]], "strip": [b'length', b'path']},
            # {"step": "status", "node": data, "command": "info clients", "expects":b'*blocked*'},{"step": "status", "node": index, "command": "info clients", "expects":b'*blocked: 0*'},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - minimize trees", "node": data, "command": "g minimize(Trees).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'665', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip": [b'length', b'path']},
            # {"step": "status", "node": data, "command": "info clients", "expects":b'*blocked*'},{"step": "status", "node": index, "command": "info clients", "expects":b'*blocked: 0*'},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - default - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'3', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'fly:Amsterdam:Dublin', b'0.5'], [b'Dublin', b'0.5'], [b'carFerry:Dublin:Paris', b'0.5'], [b'Paris', b'0.5'], [b'train:Paris:Rome', b'0.5'], [b'Rome', b'0.5']]]], "strip":[b'path']},
            # {"step": "status", "node": data, "command": "info clients", "expects":b'*blocked*'},{"step": "status", "node": index, "command": "info clients", "expects":b'*blocked: 0*'},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - minimize km - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Km).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'1119', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
            # {"step": "status", "node": data, "command": "info clients", "expects":b'*blocked*'},{"step": "status", "node": index, "command": "info clients", "expects":b'*blocked: 0*'},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - minimize euro - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Euro).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'180.5', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
            # {"step": "status", "node": data, "command": "info clients", "expects":b'*blocked*'},{"step": "status", "node": index, "command": "info clients", "expects":b'*blocked: 0*'},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - minimize trees - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Trees).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'665', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
            {"step": "Match Amsterdam->Rome - minimize time - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Time).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'665', b'object', b'Rome']], "strip":[b'path']},
            # {"step": "status", "node": data, "command": "info clients", "expects":b'*blocked*'},{"step": "status", "node": index, "command": "info clients", "expects":b'*blocked: 0*'},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Complex match", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam,Rome, Madrid,Paris,Kyiv, Warsaw,Ankara - minimize trees - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Trees).match(Amsterdam,Rome, Madrid,Paris,Kyiv, Warsaw,Ankara) ", "expects": [[b'subject', b'Amsterdam->Ankara->Amsterdam->Rome->Rome->Madrid->Madrid->Paris->Paris->Kyiv->Kyiv->Warsaw->Warsaw->Ankara', b'length', b'5816']], "strip":[b'path', b'objects']},
                        # {"step": "status", "node": data, "command": "info clients", "expects":b'*blocked*'},{"step": "status", "node": index, "command": "info clients", "expects":b'*blocked: 0*'},
]}, "clients": 100,
         },
            {"parallel": {"name": "Set Queries", "repeats": 5000, "steps": [        
            # {"step": "query some cities", "node": data, "command": "Q ((city_ascii == amsterdam) (city_ascii == seattle) (city_ascii == madrid)) & town", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
            {"step": "query some cities", "node": data, "command": "Q  Q (Amsterdam Rome Madrid Paris Kyiv Warsaw Ankara)", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
            ]}, "clients": 100
         },
        {"parallel": {"name": "Set Queries", "repeats": 5000, "steps": [        
            # {"step": "query some cities", "node": data, "command": "Q ((city_ascii == amsterdam) (city_ascii == seattle) (city_ascii == madrid)) & town", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
            {"step": "query some cities", "node": data, "command": "Q  town && (Tallinn Dublin)", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
            ]}, "clients": 100
         },
        {"parallel": {"name": "Check blocked", "repeats": 5000, "pace": 6, "steps": [        
            # {"step": "status", "node": data, "command": "info clients", "expects":b'*blocked*'},{"step": "status", "node": index, "command": "info clients", "expects":b'*blocked: 0*'},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - default", "node": data, "command": "g match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'3', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'fly:Amsterdam:Dublin', b'0.5'], [b'Dublin', b'0.5'], [b'carFerry:Dublin:Paris', b'0.5'], [b'Paris', b'0.5'], [b'train:Paris:Rome', b'0.5'], [b'Rome', b'0.5']]]], "strip": [b'length', b'path']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - minimize trees", "node": data, "command": "g minimize(Trees).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'665', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip": [b'length', b'path']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - default - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'3', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'fly:Amsterdam:Dublin', b'0.5'], [b'Dublin', b'0.5'], [b'carFerry:Dublin:Paris', b'0.5'], [b'Paris', b'0.5'], [b'train:Paris:Rome', b'0.5'], [b'Rome', b'0.5']]]], "strip":[b'path']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - minimize km - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Km).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'1119', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - minimize euro - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Euro).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'180.5', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - minimize trees - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Trees).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'665', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
            {"step": "Match Amsterdam->Rome - minimize time - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Time).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'11', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Complex match", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam,Rome, Madrid,Paris,Kyiv, Warsaw,Ankara - minimize trees - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Trees).match(Amsterdam,Rome, Madrid,Paris,Kyiv, Warsaw,Ankara) ", "expects": [[b'subject', b'Amsterdam->Ankara->Amsterdam->Rome->Rome->Madrid->Madrid->Paris->Paris->Kyiv->Kyiv->Warsaw->Warsaw->Ankara', b'length', b'5816']], "strip":[b'path', b'objects']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Set Queries", "repeats": 5000, "steps": [        
            # {"step": "query some cities", "node": data, "command": "Q ((city_ascii == amsterdam) (city_ascii == seattle) (city_ascii == madrid)) & town", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
            {"step": "query some cities", "node": data, "command": "Q (Amsterdam Rome Madrid Paris Kyiv Warsaw Ankara)", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
            ]}, "clients": 100
         },
        {"parallel": {"name": "Set Queries", "repeats": 5000, "steps": [        
            # {"step": "query some cities", "node": data, "command": "Q ((city_ascii == amsterdam) (city_ascii == seattle) (city_ascii == madrid)) & town", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
            {"step": "query some cities", "node": data, "command": "Q  town && (Tallinn Dublin)", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
            ]}, "clients": 100
         },
        {"parallel": {"name": "Check blocked", "repeats": 5000, "pace": 6, "steps": [        
            # {"step": "status", "node": data, "command": "info clients", "expects":b'*blocked*'},{"step": "status", "node": index, "command": "info clients", "expects":b'*blocked: 0*'},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - default", "node": data, "command": "g match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'3', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'fly:Amsterdam:Dublin', b'0.5'], [b'Dublin', b'0.5'], [b'carFerry:Dublin:Paris', b'0.5'], [b'Paris', b'0.5'], [b'train:Paris:Rome', b'0.5'], [b'Rome', b'0.5']]]], "strip": [b'length', b'path']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - minimize trees", "node": data, "command": "g minimize(Trees).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'665', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip": [b'length', b'path']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - default - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'3', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'fly:Amsterdam:Dublin', b'0.5'], [b'Dublin', b'0.5'], [b'carFerry:Dublin:Paris', b'0.5'], [b'Paris', b'0.5'], [b'train:Paris:Rome', b'0.5'], [b'Rome', b'0.5']]]], "strip":[b'path']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - minimize km - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Km).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'1119', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - minimize euro - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Euro).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'180.5', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - minimize trees - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Trees).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'665', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
            {"step": "Match Amsterdam->Rome - minimize time - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Time).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'11', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Complex match", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam,Rome, Madrid,Paris,Kyiv, Warsaw,Ankara - minimize trees - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Trees).match(Amsterdam,Rome, Madrid,Paris,Kyiv, Warsaw,Ankara) ", "expects": [[b'subject', b'Amsterdam->Ankara->Amsterdam->Rome->Rome->Madrid->Madrid->Paris->Paris->Kyiv->Kyiv->Warsaw->Warsaw->Ankara', b'length', b'5816']], "strip":[b'path', b'objects']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Set Queries", "repeats": 5000, "steps": [        
            # {"step": "query some cities", "node": data, "command": "Q ((city_ascii == amsterdam) (city_ascii == seattle) (city_ascii == madrid)) & town", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
            {"step": "query some cities", "node": data, "command": "Q (Amsterdam Rome Madrid Paris Kyiv Warsaw Ankara)", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
            ]}, "clients": 100
         },
        {"parallel": {"name": "Set Queries", "repeats": 5000, "steps": [        
            # {"step": "query some cities", "node": data, "command": "Q ((city_ascii == amsterdam) (city_ascii == seattle) (city_ascii == madrid)) & town", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
            {"step": "query some cities", "node": data, "command": "Q  town && (Tallinn Dublin)", "expects":[[b'key', b'Amsterdam'], [b'key', b'Madrid'], [b'key', b'Seattle']], "strip":[b'score', b'value', b'city_ascii', b'lat', b'lng', b'country', b'iso2', b'iso3', b'admin_name', b'capital', b'admin', b'population',  b'id', b'type', ]},
            ]}, "clients": 100
         },
        {"parallel": {"name": "Check blocked", "repeats": 5000, "pace": 6, "steps": [        
            # {"step": "status", "node": data, "command": "info clients", "expects":b'*blocked*'},{"step": "status", "node": index, "command": "info clients", "expects":b'*blocked: 0*'},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - default", "node": data, "command": "g match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'3', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'fly:Amsterdam:Dublin', b'0.5'], [b'Dublin', b'0.5'], [b'carFerry:Dublin:Paris', b'0.5'], [b'Paris', b'0.5'], [b'train:Paris:Rome', b'0.5'], [b'Rome', b'0.5']]]], "strip": [b'length', b'path']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - minimize trees", "node": data, "command": "g minimize(Trees).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'665', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip": [b'length', b'path']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - default - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'3', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'fly:Amsterdam:Dublin', b'0.5'], [b'Dublin', b'0.5'], [b'carFerry:Dublin:Paris', b'0.5'], [b'Paris', b'0.5'], [b'train:Paris:Rome', b'0.5'], [b'Rome', b'0.5']]]], "strip":[b'path']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - minimize km - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Km).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'1119', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - minimize euro - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Euro).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'180.5', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Traversal", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam->Rome - minimize trees - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Trees).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'665', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
            {"step": "Match Amsterdam->Rome - minimize time - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Time).match(Amsterdam,Rome) ", "expects": [[b'subject', b'Amsterdam', b'length', b'11', b'object', b'Rome', b'path', [[b'Amsterdam', b'0'], [b'train:Berlin:Amsterdam', b'328'], [b'Berlin', b'0.5'], [b'train:Prague:Berlin', b'174'], [b'Prague', b'0.5'], [b'train:Prague:Vienna', b'39'], [b'Vienna', b'0.5'], [b'train:Rome:Vienna', b'122'], [b'Rome', b'0.5']]]], "strip":[b'path']},
            ]}, "clients": 100,
         },
        {"parallel": {"name": "Complex match", "repeats": 5000, "steps": [        
            {"step": "Match Amsterdam,Rome, Madrid,Paris,Kyiv, Warsaw,Ankara - minimize trees - exclude hierarchy edged", "node": data, "command": "g exclude(admin_for,town_of).minimize(Trees).match(Amsterdam,Rome, Madrid,Paris,Kyiv, Warsaw,Ankara) ", "expects": [[b'subject', b'Amsterdam->Ankara->Amsterdam->Rome->Rome->Madrid->Madrid->Paris->Paris->Kyiv->Kyiv->Warsaw->Warsaw->Ankara', b'length', b'5816']], "strip":[b'path', b'objects']},
            ]}, "clients": 100,
         },
]
    matcher.play(scenario)

