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
        pdb.set_trace()
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


def scenario1_as_text(cluster_id, controller, data, index):
    data.execute_command("load.text", "CRLF", "TAB_SEPARATED", "FROM", "dataset1.txt", "as_text")
    RMA = data.execute_command("get", "RMA").decode('utf-8')
    assert RMA.find('identity:Rijksmuseum;') >= 0
    assert RMA.find('country:Netherlands;') >= 0
    assert RMA.find('Year_Founded:1798;') >= 0
    assert RMA.find('categories:museum,art,rembrandt,paintings;') >= 0
    SBNNO = data.execute_command("get", "SBNNO").decode('utf-8')
    assert SBNNO.find('identity:Steamboat Natchez;') >= 0
    assert SBNNO.find('country:USA;') >= 0
    assert SBNNO.find('categories:landmark,culture,transport,historic;') >= 0
    assert SBNNO.find('Year_Founded:1823;') >= 0
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
        assert HSPR_VAL.find('identity:Hermitage;') >= 0
        assert HSPR_VAL.find('country:Russia;') >= 0
        assert HSPR_VAL.find('categories:museum,art,paintings,sculptures;') >= 0
        assert HSPR_VAL.find('Year_Founded:1764;') >= 0


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
