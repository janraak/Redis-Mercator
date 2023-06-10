from __future__ import print_function
import redis
import json
import pdb
global redis_path

def scenario1_as_json(cluster_id, controller, data, index):
    data.execute_command("load.text", "CRLF", "TAB_SEPARATED", "FROM", "dataset1.txt",  "as_json")
    RMA = json.loads(data.execute_command("get", "RMA").decode('utf-8'))
    assert RMA['identity'] == 'Rijksmuseum'
    assert RMA['country'] == 'Netherlands'
    assert RMA['Year_Founded'] == '1798'
    assert RMA['categories'] == 'museum,art,rembrandt,paintings'
    SBNNO = json.loads(data.execute_command("get", "SBNNO").decode('utf-8'))
    assert SBNNO['identity'] == 'Steamboat Natchez'
    assert SBNNO['country'] == 'USA'
    assert SBNNO['categories'] == 'landmark,culture,transport,historic'
    assert SBNNO['Year_Founded'] == '1823'
    data.execute_command("rxIndex wait")
    HSPR = data.execute_command("rxquery russia 1764")
    if len(HSPR) > 0:
        HSPR = HSPR[0]  # Only first row
        assert HSPR[1+HSPR.index(b'key')] == b'HSPR'
        assert HSPR[1+HSPR.index(b'type')] == b'S'
        assert float(HSPR[1+HSPR.index(b'score')]) > 0
    HSPR = data.execute_command("rxget russia 1764")
    if len(HSPR) > 0:
        HSPR = HSPR[0]  # Only first row
        HSPR_VAL = json.loads(HSPR[1+HSPR.index(b'value')].decode('utf-8'))
        assert HSPR[1+HSPR.index(b'key')] == b'HSPR'
        assert float(HSPR[1+HSPR.index(b'score')]) > 0
        assert HSPR_VAL['identity'] == 'Hermitage'
        assert HSPR_VAL['country'] == 'Russia'
        assert HSPR_VAL['categories'] == 'museum,art,paintings,sculptures'
        assert HSPR_VAL['Year_Founded'] == '1764'
