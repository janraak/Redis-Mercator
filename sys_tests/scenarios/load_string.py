from __future__ import print_function
import redis
import json
import pdb
global redis_path



def scenario1_as_string(cluster_id, controller, data, index):
    data.execute_command("load.text", "CRLF", "TAB_SEPARATED", "FROM", "dataset1.txt", "as_string")
    RMA = data.execute_command("get", "RMA").decode('utf-8')
    assert RMA.find('Rijksmuseum') >= 0
    assert RMA.find('Netherlands') >= 0
    assert RMA.find('1798') >= 0
    assert RMA.find('museum,art,rembrandt,paintings') >= 0
    SBNNO = data.execute_command("get", "SBNNO").decode('utf-8')
    assert SBNNO.find('Steamboat Natchez') >= 0
    assert SBNNO.find('USA') >= 0
    assert SBNNO.find('landmark,culture,transport,historic') >= 0
    assert SBNNO.find('1823') >= 0
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
        HSPR_VAL = HSPR[1+HSPR.index(b'value')].decode('utf-8')
        assert HSPR[1+HSPR.index(b'key')] == b'HSPR'
        assert float(HSPR[1+HSPR.index(b'score')]) > 0
        assert HSPR_VAL.find('Hermitage') >= 0
        assert HSPR_VAL.find('Russia') >= 0
        assert HSPR_VAL.find('museum,art,paintings,sculptures') >= 0
        assert HSPR_VAL.find('1764') >= 0

