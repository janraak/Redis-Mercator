from __future__ import print_function
import redis
import json
import pdb
global redis_path

def scenario1_as_hash(cluster_id, controller, data, index):
    data.execute_command("load.text", "CRLF", "TAB_SEPARATED", "FROM", "dataset1.txt", "as_hash")
    data.execute_command("rxIndex wait")
    RMA = data.hgetall("RMA")
    try:
        assert RMA[b'identity'] == b'Rijksmuseum'
        assert RMA[b'country'] == b'Netherlands'
        assert RMA[b'Year_Founded'] == b'1798'
        assert RMA[b'categories'] == b'museum,art,rembrandt,paintings'
    except Exception as e:
        print(e)
        pass
    SBNNO = data.hgetall("SBNNO")
    assert SBNNO[b'identity'] == b'Steamboat Natchez'
    assert SBNNO[b'country'] == b'USA'
    assert SBNNO[b'categories'] == b'landmark,culture,transport,historic'
    assert SBNNO[b'Year_Founded'] == b'1823'
    HSPR = data.execute_command("rxquery russia 1764")
    if len(HSPR) > 0:
        HSPR = HSPR[0]  # Only first row
        assert HSPR[1+HSPR.index(b'key')] == b'HSPR'
        assert HSPR[1+HSPR.index(b'type')] == b'H'
        assert float(HSPR[1+HSPR.index(b'score')]) > 0
    HSPR = data.execute_command("rxget russia 1764")
    if len(HSPR) > 0:
        HSPR = HSPR[0]  # Only first row
        HSPR_VAL = HSPR[1+HSPR.index(b'value')]
        assert HSPR[1+HSPR.index(b'key')] == b'HSPR'
        assert float(HSPR[1+HSPR.index(b'score')]) > 0
        assert HSPR_VAL[1+HSPR_VAL.index(b'identity')] == b'Hermitage'
        assert HSPR_VAL[1+HSPR_VAL.index(b'country')] == b'Russia'
        assert HSPR_VAL[1+HSPR_VAL.index(b'categories')
                        ] == b'museum,art,paintings,sculptures'
        assert HSPR_VAL[1+HSPR_VAL.index(b'Year_Founded')] == b'1764'

