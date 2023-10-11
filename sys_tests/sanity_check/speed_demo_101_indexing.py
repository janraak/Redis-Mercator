from __future__ import print_function
import redis
import json
import pdb
global redis_path
def column(matrix, i):
    return [row[i] for row in matrix]

def SD_101_Non_Intrusive_Indexing_And_Full_Text_Queries(cluster_id, controller, data, index):
    print(data.execute_command("g.wget", "https://roxoft.dev/assets/dataset1.txt"))
    print(data.execute_command("load.text", "CRLF", "TAB_SEPARATED", "FROM", "dataset1.txt", "as_hash"))
    RMA = data.hgetall("RMA")
    print(RMA)
    try:
        assert RMA[b'identity'] == b'Rijksmuseum'
        assert RMA[b'country'] == b'Netherlands'
        assert RMA[b'Year_Founded'] == b'1798'
        assert RMA[b'categories'] == b'museum,art,rembrandt,paintings'
    except Exception as e:
        print(e)
        pass
    data.execute_command("rxIndex wait")
    
    query1 = data.execute_command("rxget", "science", "nature", "ranked")
    query1_keys = column(query1,1)
    try:
        assert len(query1_keys) == 7
        assert b'RMB' in query1_keys
        assert b'SI' in query1_keys
        assert b'BBJ' in query1_keys
        assert b'BNJ' in query1_keys
        assert b'CPJ' in query1_keys
        assert b'DWGNJ' in query1_keys
        assert b'PNJ' in query1_keys
    except Exception as e:
        print(e)
        pass
    
    query2 = data.execute_command("rxget", "science", "&", "nature", "ranked")
    query2_keys = column(query2,1)
    try:
        assert len(query2_keys) == 2
        assert b'RMB' in query2_keys
        assert b'SI' in query2_keys
    except Exception as e:
        print(e)
        pass
    
    query3 = data.execute_command("rxget", "year_founded", "<", "1900")
    query3_keys = column(query3,1)

    try:
        assert len(query3_keys) == 7
        assert b'APAZNO' in query3_keys
        assert b'BM' in query3_keys
        assert b'CTY' in query3_keys
        assert b'FCNO' in query3_keys
        assert b'HSPR' in query3_keys
        assert b'LP' in query3_keys
        assert b'MBAO' in query3_keys
        assert b'RMA' in query3_keys
        assert b'RSNO' in query3_keys
        assert b'SBNNO' in query3_keys
        assert b'SI' in query3_keys
        assert b'SY' in query3_keys
        assert b'TG' in query3_keys
        assert b'VCMNJ' in query3_keys
    except Exception as e:
        print(e)
        pass

