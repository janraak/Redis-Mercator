def get_dataset2():
    return [
           {   's': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 
                'o': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 
                'p': {'r':['echtgenoot','echtgenote'], 'doi':'1974-11-21'}
            },
            {   's': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 
                'o': {'k':'United States of America', 't':'country', 'abbr': 'USA'}, 
                'p': {'r':['greencard','greencard']}
            },
            {   's': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'United States of America', 't':'country', 'abbr': 'USA'}, 
                'p': {'r':['greencard','greencard']}
            },
            {   's': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 
                'o': {'k':'The Netherlands', 't':'country', 'abbr': 'NL'}, 
                'p': {'r':['passport','passport']}
            },
            {   's': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'The Netherlands', 't':'country', 'abbr': 'NL'}, 
                'p': {'r':['passport','passport']}
            },
    ]
def get_dataset0():
    return [
             {   's': {'k':'King County', 't':'county', 'abbr': 'king'}, 
                'o': {'k':'bellevue', 't': 'city'}, 
                'p': {'r':['has_town', 'town_in']}
            },
            {   's': {'k':'King County', 't':'county', 'abbr': 'king'}, 
                'o': {'k':'shoreline', 't': 'city'}, 
                'p': {'r':['has_town', 'town_in']}
            },
            {   's': {'k':'King County', 't':'county', 'abbr': 'king'}, 
                'o': {'k':'seattle', 't': 'city'}, 
                'p': {'r':['has_town', 'town_in']}
            },
            {  's': {'k':'Washington', 't': 'state'}, 
               'o': {'k':'King County', 't':'county', 'abbr': 'king'}, 
                'p': {'r':['county','state']}
            },
            {  's': {'k':'United States of America', 't':'country', 'abbr': 'USA'}, 
               'o': {'k':'Washington', 't': 'state'}, 
                'p': {'r':['county','country']}
            },
           {   's': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 
                'o': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 
                'p': {'r':['echtgenoot','echtgenote'], 'doi':'1974-11-21'}
            },
            {   's': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 
                'o': {'k':'amsterdam', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 
                'o': {'k':'enkhuizen', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 
                'o': {'k':'hoogkarspel', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 
                'o': {'k':'bellevue', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 
                'o': {'k':'seattle', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 
                'o': {'k':'shoreline', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {'s': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 'o': {'k':'vincent', 'doi':'1983-11-17', 'family': 'rock'}, 'p': ['zoon','vader']},
            {'s': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 'o': {'k':'vincent', 'doi':'1983-11-17', 'family': 'rock'}, 'p': ['zoon','moeder']},
            {   's': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'amsterdam', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'hoogkarspel', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'bellevue', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'seattle', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'shoreline', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'NoordHolland', 't': 'province', 'abbr':'NH'}, 
                'o': {'k':'oosthuizen', 't': 'city'}, 
                'p': {'r':'province'}
            },
            {  's': {'k':'Nederland', 't':'country', 'abbr': 'NL'}, 
               'o': {'k':'NoordHolland', 't': 'province', 'abbr':'NH'}, 
                'p': {'r':['province','country']}
            },
    ]
def get_dataset():
    return [
            {  's': {'k':'United States of America', 't':'country', 'abbr': 'USA'}, 
               'o': {'k':'Washington', 't': 'state'}, 
                'p': {'r':['county','country']}
            },
            {  's': {'k':'United States of America', 't':'country', 'abbr': 'USA'}, 
               'o': {'k':'Washington', 't': 'state'}, 
                'p': {'r':['county','country']}
            },
            {  's': {'k':'United States of America', 't':'country', 'abbr': 'USA'}, 
               'o': {'k':'Washington', 't': 'state'}, 
                'p': {'r':['county','country']}
            },
             {   's': {'k':'King County', 't':'county', 'abbr': 'king'}, 
                'o': {'k':'bellevue', 't': 'city'}, 
                'p': {'r':['has_town', 'town_in']}
            },
            {   's': {'k':'King County', 't':'county', 'abbr': 'king'}, 
                'o': {'k':'shoreline', 't': 'city'}, 
                'p': {'r':['has_town', 'town_in']}
            },
            {   's': {'k':'King County', 't':'county', 'abbr': 'king'}, 
                'o': {'k':'seattle', 't': 'city'}, 
                'p': {'r':['has_town', 'town_in']}
            },
            {  's': {'k':'Washington', 't': 'state'}, 
               'o': {'k':'King County', 't':'county', 'abbr': 'king'}, 
                'p': {'r':['county','state']}
            },
            {  's': {'k':'United States of America', 't':'country', 'abbr': 'USA'}, 
               'o': {'k':'Washington', 't': 'state'}, 
                'p': {'r':['county','country']}
            },
            {   's': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 
                'o': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 
                'p': {'r':['echtgenoot','echtgenote'], 'doi':'1974-11-21'}
            },
            {   's': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 
                'o': {'k':'amsterdam', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 
                'o': {'k':'enkhuizen', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 
                'o': {'k':'hoogkarspel', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 
                'o': {'k':'bellevue', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 
                'o': {'k':'seattle', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 
                'o': {'k':'shoreline', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {'s': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 'o': {'k':'vincent', 'doi':'1983-11-17', 'family': 'rock'}, 'p': ['zoon','vader']},
            {'s': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 'o': {'k':'vincent', 'doi':'1983-11-17', 'family': 'rock'}, 'p': ['zoon','moeder']},
            {   's': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'amsterdam', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'hoogkarspel', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'bellevue', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'seattle', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'shoreline', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {'s': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 'o': {'k':'marloes', 'doi':'1978-12-09', 'family': 'rock'}, 'p': ['dochter','vader']},
            {'s': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 'o': {'k':'marloes', 'doi':'1978-12-09', 'family': 'rock'}, 'p': ['dochter','moeder']},
            {'s': {'k':'william', 'family': 'horst'}, 'o': {'k':'johanna', 'family': 'loevesijn'}, 'p': ['echtgenoot','echtgenote']},
            {   's': {'k':'william', 'family': 'horst'}, 
                'o': {'k':'amsterdam', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {'s': {'k':'william', 'family': 'horst'}, 'o': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 'p': ['dochter','vader']},
            {'s': {'k':'johanna', 'family': 'loevesijn'}, 'o': {'k':'clara', 'doi':'1955-11-23', 'family': 'horst'}, 'p': ['dochter','moeder']},
            {   's': {'k':'johanna', 'family': 'loevesijn'}, 
                'o': {'k':'amsterdam', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'rein', 'doi':'1928-04-29', 'family': 'rock'}, 
                'o': {'k':'fannie', 'doi':'1930-05-24', 'family': 'sjoukens'}, 
                'p': {"r": ['echtgenoot','echtgenote'], 'doi':'1953-04-29'}
            },
            {'s': {'k':'rein', 'doi':'1928-04-29', 'family': 'rock'}, 'o': {'k':'john', 'doi':'1953-11-18', 'family': 'rock'}, 'p': ['zoon','vader']},
            {'s': {'k':'fannie', 'doi':'1930-05-24', 'family': 'sjoukens'}, 'o': 'john', 'p': ['zoon','moeder']},
            {   's': {'k':'rein', 'doi':'1928-04-29', 'family': 'rock'}, 
                'o': {'k':'frans', 'doi':'1961-10-24', 'family': 'rock'}, 
                'p': ['zoon','vader']}, 
            {   's': {'k':'fannie', 'doi':'1930-05-24', 'family': 'sjoukens'}, 
                'o': {'k':'frans', 'doi':'1961-10-24', 'family': 'rock'}, 
                'p': ['zoon','moeder']},
            {   's': {'k':'rein', 'doi':'1928-04-29', 'family': 'rock'}, 
                'o': {'k':'jolanda', 'doi':'1965-11-15', 'family': 'rock'}, 
                'p': ['dochter','vader']},
            {   's': {'k':'fannie', 'doi':'1930-05-24', 'family': 'sjoukens'}, 
                'o': {'k':'jolanda', 'doi':'1965-11-15', 'family': 'rock'}, 
                'p': ['dochter','moeder']},
            {   's': {'k':'frans', 'doi':'1961-10-24', 'family': 'rock'}, 
                'o': 'tabitha', 
                'p': ['echtgenoot','echtgenote']},
            {   's': {'k':'martin', 'family': 'vanscheijen'}, 
                'o': {'k':'jolanda', 'doi':'1965-11-15', 'family': 'rock'}, 
                'p': {'r':['echtgenoot','echtgenote'], 'doi':'1987-05-18'},
            },
            {'s': {'k':'frans', 'doi':'1961-10-24', 'family': 'rock'}, 'o': {'k':'jerom', 'family': 'rock'}, 'p': ['zoon','vader']},
            {'s': 'tabitha', 'o': {'k':'jerom', 'family': 'rock'}, 'p': ['zoon','moeder']},
            {'s': {'k':'martin', 'family': 'vanscheijen'}, 'o': {'k':'quint', 'family': 'vanscheijen'}, 'p': ['zoon','vader']},
            {'s': {'k':'jolanda', 'doi':'1965-11-15', 'family': 'rock'}, 'o': {'k':'quint', 'family': 'vanscheijen'}, 'p': ['zoon','moeder']},
            {'o': {'k':'marloes', 'doi':'1978-12-09', 'family': 'rock'}, 's': {'k':'jeff', 'family': 'vanhartingsvelt'}, 'p': ['echtgenoot','echtgenote']},
            {'s': {'k':'marloes', 'doi':'1978-12-09', 'family': 'rock'}, 'o': 'danique', 'p': ['zoon','moeder']},
            {'s': {'k':'marloes', 'doi':'1978-12-09', 'family': 'rock'}, 'o': 'luciano', 'p': ['zoon','moeder']},
            {'s': {'k':'quint', 'family': 'vanscheijen'}, 'o': 'shannen', 'p': ['echtgenoot','echtgenote']},
            {'s': {'k':'quint', 'family': 'vanscheijen'}, 'o': {'k':'harvey', 'family': 'vanscheijen'}, 'p': ['zoon','vader']},
            {'s': 'shannen', 'o': {'k':'harvey', 'family': 'vanscheijen'}, 'p': ['zoon','moeder']},
            {'s': {'k':'jeff', 'family': 'vanhartingsvelt'}, 'o': {'k':'jasper', 'family': 'vanhartingsvelt'}, 'p': ['zoon','vader']},
            {'s': {'k':'jeff', 'family': 'vanhartingsvelt'}, 'o': {'k':'ryan', 'family': 'vanhartingsvelt'}, 'p': ['zoon','vader']},
            {'s': {'k':'jeff', 'family': 'vanhartingsvelt'}, 'o': {'k':'siemon', 'family': 'vanhartingsvelt'}, 'p': ['zoon','vader']},
            {   's': {'k':'North Holland', 't': 'province', 'abbr':'NH'}, 
                'o': {'k':'amsterdam', 't': 'city'}, 
                'p': {'r':['gemeente','provincie']}
            },
            {   's': {'k':'North Holland', 't': 'province', 'abbr':'NH'}, 
                'o': {'k':'enkhuizen', 't': 'city'}, 
                'p': {'r':['gemeente','provincie']}
            },
            {   's': {'k':'North Holland', 't': 'province', 'abbr':'NH'}, 
                'o': {'k':'hoogkarspel', 't': 'city'}, 
                'p': {'r':['gemeente','provincie']}
            },
            {   's': {'k':'North Holland', 't': 'province', 'abbr':'NH'}, 
                'o': {'k':'hoogkarspel', 't': 'city'}, 
                'p': {'r':['gemeente','provincie']}
            },
            {   's': {'k':'North Holland', 't': 'province', 'abbr':'NH'}, 
                'o': {'k':'oosthuizen', 't': 'city'}, 
                'p': {'r':'province'}
            },
            {   's': {'k':'King County', 't':'county', 'abbr': 'king'}, 
                'o': {'k':'bellevue', 't': 'city'}, 
                'p': {'r':['has_town', 'town_in']}
            },
            {   's': {'k':'King County', 't':'county', 'abbr': 'king'}, 
                'o': {'k':'shoreline', 't': 'city'}, 
                'p': {'r':['has_town', 'town_in']}
            },
            {   's': {'k':'King County', 't':'county', 'abbr': 'king'}, 
                'o': {'k':'seattle', 't': 'city'}, 
                'p': {'r':['has_town', 'town_in']}
            },
            {  's': {'k':'Washington', 't': 'state'}, 
               'o': {'k':'King County', 't':'county', 'abbr': 'king'}, 
                'p': {'r':['county','state']}
            },
            {  's': {'k':'United States of America', 't':'country', 'abbr': 'USA'}, 
               'o': {'k':'Washington', 't': 'state'}, 
                'p': {'r':['county','country']}
            },
            {  's': {'k':'Nederland', 't':'country', 'abbr': 'NL'}, 
               'o': {'k':'North Holland', 't': 'province', 'abbr':'NH'}, 
                'p': {'r':['province','country']}
            },
        ]
