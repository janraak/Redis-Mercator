def get_dataset2():
    return [
           {   's': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 
                'o': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 
                'p': {'r':['echtgenoot','echtgenote'], 'doi':'1974-11-21'}
            },
            {   's': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 
                'o': {'k':'United States of America', 't':'country', 'abbr': 'USA'}, 
                'p': {'r':['greencard','greencard']}
            },
            {   's': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'United States of America', 't':'country', 'abbr': 'USA'}, 
                'p': {'r':['greencard','greencard']}
            },
            {   's': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 
                'o': {'k':'The Netherlands', 't':'country', 'abbr': 'NL'}, 
                'p': {'r':['passport','passport']}
            },
            {   's': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 
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
           {   's': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 
                'o': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 
                'p': {'r':['echtgenoot','echtgenote'], 'doi':'1974-11-21'}
            },
            {   's': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 
                'o': {'k':'amsterdam', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 
                'o': {'k':'enkhuizen', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 
                'o': {'k':'hoogkarspel', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 
                'o': {'k':'bellevue', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 
                'o': {'k':'seattle', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 
                'o': {'k':'shoreline', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {'s': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 'o': {'k':'vincent', 'doi':'1983-11-17', 'family': 'raak'}, 'p': ['zoon','vader']},
            {'s': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 'o': {'k':'vincent', 'doi':'1983-11-17', 'family': 'raak'}, 'p': ['zoon','moeder']},
            {   's': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'amsterdam', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'hoogkarspel', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'bellevue', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'seattle', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 
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
            {   's': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 
                'o': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 
                'p': {'r':['echtgenoot','echtgenote'], 'doi':'1974-11-21'}
            },
            {   's': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 
                'o': {'k':'amsterdam', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 
                'o': {'k':'enkhuizen', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 
                'o': {'k':'hoogkarspel', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 
                'o': {'k':'bellevue', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 
                'o': {'k':'seattle', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 
                'o': {'k':'shoreline', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {'s': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 'o': {'k':'vincent', 'doi':'1983-11-17', 'family': 'raak'}, 'p': ['zoon','vader']},
            {'s': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 'o': {'k':'vincent', 'doi':'1983-11-17', 'family': 'raak'}, 'p': ['zoon','moeder']},
            {   's': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'amsterdam', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'hoogkarspel', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'bellevue', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'seattle', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 
                'o': {'k':'shoreline', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {'s': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 'o': {'k':'marloes', 'doi':'1978-12-09', 'family': 'raak'}, 'p': ['dochter','vader']},
            {'s': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 'o': {'k':'marloes', 'doi':'1978-12-09', 'family': 'raak'}, 'p': ['dochter','moeder']},
            {'s': {'k':'wim', 'family': 'horst'}, 'o': {'k':'bep', 'family': 'loevesijn'}, 'p': ['echtgenoot','echtgenote']},
            {   's': {'k':'wim', 'family': 'horst'}, 
                'o': {'k':'amsterdam', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {'s': {'k':'wim', 'family': 'horst'}, 'o': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 'p': ['dochter','vader']},
            {'s': {'k':'bep', 'family': 'loevesijn'}, 'o': {'k':'isabella', 'doi':'1955-11-23', 'family': 'horst'}, 'p': ['dochter','moeder']},
            {   's': {'k':'bep', 'family': 'loevesijn'}, 
                'o': {'k':'amsterdam', 't': 'city'}, 
                'p': {'r':['inwoner_van', 'woonplaats_van']}
            },
            {   's': {'k':'rein', 'doi':'1928-04-29', 'family': 'raak'}, 
                'o': {'k':'fannie', 'doi':'1930-05-24', 'family': 'sjoukens'}, 
                'p': {"r": ['echtgenoot','echtgenote'], 'doi':'1953-04-29'}
            },
            {'s': {'k':'rein', 'doi':'1928-04-29', 'family': 'raak'}, 'o': {'k':'jan', 'doi':'1953-11-18', 'family': 'raak'}, 'p': ['zoon','vader']},
            {'s': {'k':'fannie', 'doi':'1930-05-24', 'family': 'sjoukens'}, 'o': 'jan', 'p': ['zoon','moeder']},
            {   's': {'k':'rein', 'doi':'1928-04-29', 'family': 'raak'}, 
                'o': {'k':'frans', 'doi':'1961-10-24', 'family': 'raak'}, 
                'p': ['zoon','vader']}, 
            {   's': {'k':'fannie', 'doi':'1930-05-24', 'family': 'sjoukens'}, 
                'o': {'k':'frans', 'doi':'1961-10-24', 'family': 'raak'}, 
                'p': ['zoon','moeder']},
            {   's': {'k':'rein', 'doi':'1928-04-29', 'family': 'raak'}, 
                'o': {'k':'jolanda', 'doi':'1965-11-15', 'family': 'raak'}, 
                'p': ['dochter','vader']},
            {   's': {'k':'fannie', 'doi':'1930-05-24', 'family': 'sjoukens'}, 
                'o': {'k':'jolanda', 'doi':'1965-11-15', 'family': 'raak'}, 
                'p': ['dochter','moeder']},
            {   's': {'k':'frans', 'doi':'1961-10-24', 'family': 'raak'}, 
                'o': 'tabitha', 
                'p': ['echtgenoot','echtgenote']},
            {   's': {'k':'martin', 'family': 'vanscheijen'}, 
                'o': {'k':'jolanda', 'doi':'1965-11-15', 'family': 'raak'}, 
                'p': {'r':['echtgenoot','echtgenote'], 'doi':'1987-05-18'},
            },
            {'s': {'k':'frans', 'doi':'1961-10-24', 'family': 'raak'}, 'o': {'k':'jerom', 'family': 'raak'}, 'p': ['zoon','vader']},
            {'s': 'tabitha', 'o': {'k':'jerom', 'family': 'raak'}, 'p': ['zoon','moeder']},
            {'s': {'k':'martin', 'family': 'vanscheijen'}, 'o': {'k':'quint', 'family': 'vanscheijen'}, 'p': ['zoon','vader']},
            {'s': {'k':'jolanda', 'doi':'1965-11-15', 'family': 'raak'}, 'o': {'k':'quint', 'family': 'vanscheijen'}, 'p': ['zoon','moeder']},
            {'o': {'k':'marloes', 'doi':'1978-12-09', 'family': 'raak'}, 's': {'k':'jeff', 'family': 'vanhartingsvelt'}, 'p': ['echtgenoot','echtgenote']},
            {'s': {'k':'marloes', 'doi':'1978-12-09', 'family': 'raak'}, 'o': 'danique', 'p': ['zoon','moeder']},
            {'s': {'k':'marloes', 'doi':'1978-12-09', 'family': 'raak'}, 'o': 'luciano', 'p': ['zoon','moeder']},
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
