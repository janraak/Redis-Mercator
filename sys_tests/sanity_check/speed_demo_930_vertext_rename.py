from __future__ import print_function
from colorama import Fore, Back, Style
import time
import redis
import json
import pdb
from match import Matcher, HeaderOnly, find_local_ip, ReconnectException
import pdb


def SD_930_Vertex_rename(cluster_id, controller, data, index):
    matcher = Matcher("SD_930_Vertex_rename")
    matcher.by_strings("TURN OFF INDEXING", data.execute_command("RXINDEX", "OFF"), b"OK")
    matcher.by_sets(
        "ADD TEST DATA",
        data.execute_command(
            "g",
            "addv(J,man).addv(I,vrouw).reset.predicate(man_van,vrouw_van).subject(J).object(I)",
        ),
        [
            b"key",
            b"man_van:J:I",
            b"score",
            b"1",
            b"value",
            b"type",
            b"man_van",
            b"half",
            b"vrouw_van:I:J",
            b"edge",
            b"man_van:J:I",
            b"key",
            b"vrouw_van:I:J",
            b"score",
            b"1",
            b"value",
            b"type",
            b"vrouw_van",
            b"half",
            b"vrouw_van:I:J",
            b"edge",
            b"man_van:J:I",
        ],
    )

    matcher.by_sets(
        "Matched J->I",
        data.execute_command("g", "match(J,I)"),
        [
            [
                b"subject",
                b"J",
                b"length",
                b"1",
                b"object",
                b"I",
                b"path",
                [[b"J", b"0"], [b"vrouw_van:I:J", b"0.5"], [b"I", b"0.5"]],
            ]
        ],
    )

    matcher.by_sets(
        "Matched I->J",
        data.execute_command("g", "match(I, J)"),
        [[[b'subject', b'I', b'length', b'1', b'object', b'J', b'path', [[b'I', b'0'], [b'vrouw_van:I:J', b'0.5'], [b'J', b'0.5']]]],
         [[b'subject', b'I', b'length', b'1', b'object', b'J', b'path', [[b'I', b'0'], [b'man_van:J:I', b'0.5'], [b'J', b'0.5']]]]],
    )

    matcher.by_sets(
        "By Type: man",
        data.execute_command("g", "v(man)"),
        [[b'key', b'J', b'score', b'1', b'value', [b'type', b'man']]]
    )

    matcher.by_sets(
        "By Type: vrouw_van",
        data.execute_command("g", "e(vrouw_van)"),
        [[b'key', b'vrouw_van:I:J', b'score', b'1', b'value', [b'type', b'vrouw_van', b'half', b'vrouw_van:I:J', b'edge', b'man_van:J:I']]]
    )

    matcher.by_sets(
        "By Type: man_van",
        data.execute_command("g", "e(man_van)"),
        [[b'key', b'man_van:J:I', b'score', b'1', b'value', [b'type', b'man_van', b'half', b'vrouw_van:I:J', b'edge', b'man_van:J:I']]]
    )

    matcher.by_sets(
        "check keys before",
        data.execute_command("keys", "*"),
        [
            b"`vrouw",
            b"^vrouw_van:I:J",
            b"^I",
            b"~man_van",
            b"^man_van:J:I",
            b"`man",
            b"^J",
            b"I",
            b"J",
            b"man_van:J:I",
            b"vrouw_van:I:J",
            b"~vrouw_van",
        ],
    )
    matcher.by_sets(
        "check members before `man",
        data.execute_command("smembers", "`man"),
        set({b"J"}),
    )
    matcher.by_sets(
        "check members before ~man_van",
        data.execute_command("smembers", "~man_van"),
        {b"man_van:J:I"},
    )
    matcher.by_sets(
        "check members before ~vrouw_van",
        data.execute_command("smembers", "~vrouw_van"),
        {b"vrouw_van:I:J"},
    )
    matcher.by_sets(
        "check members before ^J",
        data.execute_command("smembers", "^J"),
        {
            b"vrouw_van|^vrouw_van:I:J|OE|1.000000",
            b"man_van|^man_van:J:I|SE|1.000000|+",
        },
    )
    matcher.by_sets(
        "check members before ^vrouw_van:I:J",
        data.execute_command("smembers", "^vrouw_van:I:J"),
        {b"vrouw_van|^J|EO|1.000000", b"vrouw_van|^I|ES|1.000000"},
    )

    matcher.by_sets(
        "check members before man_van:J:I",
        data.execute_command("smembers", "^man_van:J:I"),
        {b"man_van|^I|EO|1.000000", b"man_van|^J|ES|1.000000"},
    )

    matcher.by_sets(
        "check members before ^I",
        data.execute_command("smembers", "^I"),
        {
            b"vrouw_van|^vrouw_van:I:J|SE|1.000000|+",
            b"man_van|^man_van:J:I|OE|1.000000",
        },
    )

    matcher.by_strings(
        "RENAME VERTEX", data.execute_command("g", "renameKeys(J,johannes)"), b"No results!"
    )

    matcher.by_sets(
        "check keys after ",
        data.execute_command("keys", "*"),
        [
            b"I",
            b"^vrouw_van:I:johannes",
            b"johannes",
            b"`man",
            b"`vrouw",
            b"^johannes",
            b"vrouw_van:I:johannes",
            b"^man_van:johannes:I",
            b"man_van:johannes:I",
            b"^I",
            b"~vrouw_van",
            b"~man_van",
        ],
    )

    matcher.by_sets(
        "check members after  `man",
        data.execute_command("smembers", "`man"),
        set({b"johannes"}),
    )
    matcher.by_sets(
        "check members after  ~man_van",
        data.execute_command("smembers", "~man_van"),
        {b"man_van:johannes:I"},
    )
    matcher.by_sets(
        "check members after  ~vrouw_van",
        data.execute_command("smembers", "~vrouw_van"),
        {b"vrouw_van:I:johannes"},
    )
    matcher.by_sets(
        "check members after  ^johannes",
        data.execute_command("smembers", "^johannes"),
        {
            b"vrouw_van|^vrouw_van:I:johannes|OE|1.000000",
            b"man_van|^man_van:johannes:I|SE|1.000000|+",
        },
    )
    matcher.by_sets(
        "check members after  ^vrouw_van:I:johannes",
        data.execute_command("smembers", "^vrouw_van:I:johannes"),
        {b"vrouw_van|^johannes|EO|1.000000", b"vrouw_van|^I|ES|1.000000"},
    )

    matcher.by_sets(
        "check members after  man_van:johannes:I",
        data.execute_command("smembers", "^man_van:johannes:I"),
        {b"man_van|^I|EO|1.000000", b"man_van|^johannes|ES|1.000000"},
    )

    matcher.by_sets(
        "check members after ^I",
        data.execute_command("smembers", "^I"),
        {
            b"vrouw_van|^vrouw_van:I:johannes|SE|1.000000|+",
            b"man_van|^man_van:johannes:I|OE|1.000000",
        },
    )

    matcher.by_sets(
        "Matched after johannes->I",
        data.execute_command("g", "match(johannes,I)"),
        [[
            [
                b"subject",
                b"johannes",
                b"length",
                b"1",
                b"object",
                b"I",
                b"path",
                [[b"johannes", b"0"], [b"vrouw_van:I:johannes", b"0.5"], [b"I", b"0.5"]],
            ]
        ],[[b'subject', b'johannes', b'length', b'1', b'object', b'I', b'path', [[b'johannes', b'0'], [b'man_van:johannes:I', b'0.5'], [b'I', b'0.5']]]]]
    )
    matcher.by_sets(
        "Matched I->johannes",
        data.execute_command("g", "match(I, johannes)"),
        [[
            [
                b"subject",
                b"johannes",
                b"length",
                b"1",
                b"object",
                b"I",
                b"path",
                [[b"johannes", b"0"], [b"vrouw_van:I:johannes", b"0.5"], [b"I", b"0.5"]],
            ]
        ],[[b'subject', b'I', b'length', b'1', b'object', b'johannes', b'path', [[b'I', b'0'], [b'man_van:johannes:I', b'0.5'], [b'johannes', b'0.5']]]]]
    )

    matcher.by_sets(
        "By Type: man",
        data.execute_command("g", "v(man)"),
        [[b'key', b'johannes', b'score', b'1', b'value', [b'type', b'man']]]
    )

    matcher.by_sets(
        "By Type: vrouw_van",
        data.execute_command("g", "e(vrouw_van)"),
        [[b'key', b'vrouw_van:I:johannes', b'score', b'1', b'value', [b'type', b'vrouw_van', b'half', b'vrouw_van:I:J', b'edge', b'man_van:J:I']]]
    )

    matcher.by_sets(
        "By Type: man_van",
        data.execute_command("g", "e(man_van)"),
        [[b'key', b'man_van:johannes:I', b'score', b'1', b'value', [b'type', b'man_van', b'half', b'vrouw_van:I:J', b'edge', b'man_van:J:I']]]
    )

    matcher.by_strings(
        "RENAME VERTEX", data.execute_command("g", "renameKeys(johannes,J)"), b"No results!"
    )


    matcher.by_sets(
        "Matched reversed J->I",
        data.execute_command("g", "match(J,I)"),
        [
            [[b'subject', b'J', b'length', b'1', b'object', b'I', b'path', [[b'J', b'0'], [b'man_van:J:I', b'0.5'], [b'I', b'0.5']]]],
            [[b'subject', b'J', b'length', b'1', b'object', b'I', b'path', [[b'J', b'0'], [b'vrouw_van:I:J', b'0.5'], [b'I', b'0.5']]]]
        ],
    )

    matcher.by_sets(
        "Matched reversed I->J",
        data.execute_command("g", "match(I, J)"),
        [[[b'subject', b'I', b'length', b'1', b'object', b'J', b'path', [[b'I', b'0'], [b'vrouw_van:I:J', b'0.5'], [b'J', b'0.5']]]],
         [[b'subject', b'I', b'length', b'1', b'object', b'J', b'path', [[b'I', b'0'], [b'man_van:J:I', b'0.5'], [b'J', b'0.5']]]]],
    )

    matcher.by_sets(
        "By Type: reversed man",
        data.execute_command("g", "v(man)"),
        [[b'key', b'J', b'score', b'1', b'value', [b'type', b'man']]]
    )

    matcher.by_sets(
        "By Type: reversed vrouw_van",
        data.execute_command("g", "e(vrouw_van)"),
        [[b'key', b'vrouw_van:I:J', b'score', b'1', b'value', [b'type', b'vrouw_van', b'half', b'vrouw_van:I:J', b'edge', b'man_van:J:I']]]
    )

    matcher.by_sets(
        "By Type: reversed man_van",
        data.execute_command("g", "e(man_van)"),
        [[b'key', b'man_van:J:I', b'score', b'1', b'value', [b'type', b'man_van', b'half', b'vrouw_van:I:J', b'edge', b'man_van:J:I']]]
    )

    matcher.by_sets(
        "check keys  reversed",
        data.execute_command("keys", "*"),
        [
            b"`vrouw",
            b"^vrouw_van:I:J",
            b"^I",
            b"~man_van",
            b"^man_van:J:I",
            b"`man",
            b"^J",
            b"I",
            b"J",
            b"man_van:J:I",
            b"vrouw_van:I:J",
            b"~vrouw_van",
        ],
    )
    matcher.by_sets(
        "check members  reversed `man",
        data.execute_command("smembers", "`man"),
        set({b"J"}),
    )
    matcher.by_sets(
        "check members  reversed ~man_van",
        data.execute_command("smembers", "~man_van"),
        {b"man_van:J:I"},
    )
    matcher.by_sets(
        "check members  reversed ~vrouw_van",
        data.execute_command("smembers", "~vrouw_van"),
        {b"vrouw_van:I:J"},
    )
    matcher.by_sets(
        "check members  reversed ^J",
        data.execute_command("smembers", "^J"),
        {
            b"vrouw_van|^vrouw_van:I:J|OE|1.000000",
            b"man_van|^man_van:J:I|SE|1.000000|+",
        },
    )
    matcher.by_sets(
        "check members  reversed ^vrouw_van:I:J",
        data.execute_command("smembers", "^vrouw_van:I:J"),
        {b"vrouw_van|^J|EO|1.000000", b"vrouw_van|^I|ES|1.000000"},
    )

    matcher.by_sets(
        "check members  reversed man_van:J:I",
        data.execute_command("smembers", "^man_van:J:I"),
        {b"man_van|^I|EO|1.000000", b"man_van|^J|ES|1.000000"},
    )

    matcher.by_sets(
        "check members  reversed ^I",
        data.execute_command("smembers", "^I"),
        {
            b"vrouw_van|^vrouw_van:I:J|SE|1.000000|+",
            b"man_van|^man_van:J:I|OE|1.000000",
        },
    )


    matcher.by_strings(
        "RENAME VERTEX", data.execute_command("g", "renameKeys(I,IR)"), b"No results!"
    )
    matcher.by_strings("TURN ON INDEXING", data.execute_command("RXINDEX", "ON"), b"OK")
    matcher.finalize()


def SD_940_Swap_keys(cluster_id, controller, data, index):
    matcher = Matcher("SD_940_Swap_keys[graph]")
    scenario = [
        {"step": "ADD TEST DATA C1xS1", "node": data, "command": "g addv(C1,cluster).addv(S1,server).reset.predicate(I_FOR,I_OF).subject(C1).object(S1)", "expects": [[b'key', b'I_FOR:C1:S1', b'score', b'1', b'value', [b'type', b'I_FOR', b'half', b'I_OF:S1:C1', b'edge', b'I_FOR:C1:S1']], [b'key', b'I_OF:S1:C1', b'score', b'1', b'value', [b'type', b'I_OF', b'half', b'I_OF:S1:C1', b'edge', b'I_FOR:C1:S1']]]},
        {"step": "ADD TEST DATA C1xS2", "node": data, "command": "g addv(C1,cluster).addv(S2,server).reset.predicate(I_FOR,I_OF).subject(C1).object(S2)", "expects": [[b'key', b'I_FOR:C1:S2', b'score', b'1', b'value', [b'type', b'I_FOR', b'half', b'I_OF:S2:C1', b'edge', b'I_FOR:C1:S2']], [b'key', b'I_OF:S2:C1', b'score', b'1', b'value', [b'type', b'I_OF', b'half', b'I_OF:S2:C1', b'edge', b'I_FOR:C1:S2']]]},
        {"step": "ADD TEST DATA C2xS3", "node": data, "command": "g addv(C2,cluster).addv(S3,server).reset.predicate(I_FOR,I_OF).subject(C2).object(S3)", "expects": [[b'key', b'I_FOR:C2:S3', b'score', b'1', b'value', [b'type', b'I_FOR', b'half', b'I_OF:S3:C2', b'edge', b'I_FOR:C2:S3']], [b'key', b'I_OF:S3:C2', b'score', b'1', b'value', [b'type', b'I_OF', b'half', b'I_OF:S3:C2', b'edge', b'I_FOR:C2:S3']]]},
        {"step": "ADD TEST DATA C2xS4", "node": data, "command": "g addv(C2,cluster).addv(S4,server).reset.predicate(I_FOR,I_OF).subject(C2).object(S4)", "expects": [[b'key', b'I_FOR:C2:S4', b'score', b'1', b'value', [b'type', b'I_FOR', b'half', b'I_OF:S4:C2', b'edge', b'I_FOR:C2:S4']], [b'key', b'I_OF:S4:C2', b'score', b'1', b'value', [b'type', b'I_OF', b'half', b'I_OF:S4:C2', b'edge', b'I_FOR:C2:S4']]]},
        {"step": "MATCH C1 and C2 before first cluster swap", "node":data, "command": "g match(C2,C1)", "expects": []},
        {"step": "MATCH S1 and S2 before first cluster swap", "node":data, "command": "g match(S1,S2)", "expects": [[b'subject', b'S1', b'length', b'2', b'object', b'S2', b'path', [[b'S1', b'0'], [b'I_FOR:C1:S1', b'0.5'], [b'C1', b'0.5'], [b'I_OF:S2:C1', b'0.5'], [b'S2', b'0.5']]]]},
        {"step": "MATCH S1 and S3 before first cluster swap", "node":data, "command": "g match(S1,S3)", "expects": []},
        {"step": "MATCH S3 and S4 before first cluster swap", "node":data, "command": "g match(S3,S4)", "expects": [[[b'subject', b'S3', b'length', b'2', b'object', b'S4', b'path', [[b'S3', b'0'], [b'I_OF:S3:C2', b'0.5'], [b'C2', b'0.5'], [b'I_FOR:C2:S4', b'0.5'], [b'S4', b'0.5']]]], [[b'subject', b'S3', b'length', b'2', b'object', b'S4', b'path', [[b'S3', b'0'], [b'I_FOR:C2:S3', b'0.5'], [b'C2', b'0.5'], [b'I_OF:S4:C2', b'0.5'], [b'S4', b'0.5']]]]]},

        {"step": "SWAP clusters C1 and C2", "node":data, "command": "g swapkeys(C1,C2)", "expects": [b'No results!']},
        {"step": "MATCH C1 and C2 after first cluster swap", "node":data, "command": "g match(C2,C1)", "expects": []},
        {"step": "MATCH S1 and S2 after first cluster swap", "node":data, "command": "g match(S1,S2)", "expects": [[b'subject', b'S1', b'length', b'2', b'object', b'S2', b'path', [[b'S1', b'0'], [b'I_OF:S1:C2', b'0.5'], [b'C2', b'0.5'], [b'I_OF:S2:C2', b'0.5'], [b'S2', b'0.5']]]]},
        {"step": "MATCH S1 and S3 after first cluster swap", "node":data, "command": "g match(S1,S3)", "expects": []},
        {"step": "MATCH S3 and S4 after first cluster swap", "node":data, "command": "g match(S3,S4)", "expects": [[[b'subject', b'S3', b'length', b'2', b'object', b'S4', b'path', [[b'S3', b'0'], [b'I_OF:S3:C1', b'0.5'], [b'C1', b'0.5'], [b'I_FOR:C1:S4', b'0.5'], [b'S4', b'0.5']]]], [[b'subject', b'S3', b'length', b'2', b'object', b'S4', b'path', [[b'S3', b'0'], [b'I_FOR:C1:S3', b'0.5'], [b'C1', b'0.5'], [b'I_OF:S4:C1', b'0.5'], [b'S4', b'0.5']]]]]},
        {"step": "SWAP clusters C1 and C2", "node":data, "command": "g swapkeys(C2,C1)", "expects": [b'No results!']},
        {"step": "MATCH C1 and C2 after second cluster swap", "node":data, "command": "g match(C2,C1)", "expects": []},
        {"step": "MATCH S1 and S2 after second cluster swap", "node":data, "command": "g match(S1,S2)", "expects": [[b'subject', b'S1', b'length', b'2', b'object', b'S2', b'path', [[b'S1', b'0'], [b'I_FOR:C1:S1', b'0.5'], [b'C1', b'0.5'], [b'I_OF:S2:C1', b'0.5'], [b'S2', b'0.5']]]]},
        {"step": "MATCH S1 and S3 after second cluster swap", "node":data, "command": "g match(S1,S3)", "expects": []},
        {"step": "MATCH S3 and S4 after second cluster swap", "node":data, "command": "g match(S3,S4)", "expects": [[[b'subject', b'S3', b'length', b'2', b'object', b'S4', b'path', [[b'S3', b'0'], [b'I_OF:S3:C2', b'0.5'], [b'C2', b'0.5'], [b'I_FOR:C2:S4', b'0.5'], [b'S4', b'0.5']]]], [[b'subject', b'S3', b'length', b'2', b'object', b'S4', b'path', [[b'S3', b'0'], [b'I_FOR:C2:S3', b'0.5'], [b'C2', b'0.5'], [b'I_OF:S4:C2', b'0.5'], [b'S4', b'0.5']]]]]},
        {"step": "FUSE C1 before first server  swap", "node":data, "command": "g V(C1).fuseout(I_OF)", "expects": [[b'value', [b'subject', b'C1', b'object', b'C1', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S1:C1', b'edge', b'I_FOR:C1:S1']], [b'value', [b'subject', b'C1', b'object', b'C1', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S2:C1', b'edge', b'I_FOR:C1:S2']]], "strip":[b"key", b"score"]},
        {"step": "FUSE C2 before first server  swap", "node":data, "command": "g V(C2).fuseout(I_OF)", "expects": [[b'value', [b'subject', b'C2', b'object', b'C2', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S4:C2', b'edge', b'I_FOR:C2:S4']], [b'value', [b'subject', b'C2', b'object', b'C2', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S3:C2', b'edge', b'I_FOR:C2:S3']]], "strip":[b"key", b"score"]},
        {"step": "SWAP servers S1 and S3", "node":data, "command": "g swapkeys(S1,S3)", "expects": [b'No results!']},
        {"step": "FUSE C1 after first server  swap", "node":data, "command": "g V(C1).fuseout(I_OF)", "expects": [[b'value', [b'subject', b'C1', b'object', b'C1', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S1:C1', b'edge', b'I_FOR:C1:S1']], [b'value', [b'subject', b'C1', b'object', b'C1', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S2:C1', b'edge', b'I_FOR:C1:S2']]], "strip":[b"key", b"score"]},
        {"step": "FUSE C2 after first server  swap", "node":data, "command": "g V(C2).fuseout(I_OF)", "expects": [[b'value', [b'subject', b'C2', b'object', b'C2', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S4:C2', b'edge', b'I_FOR:C2:S4']], [b'value', [b'subject', b'C2', b'object', b'C2', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S3:C2', b'edge', b'I_FOR:C2:S3']]], "strip":[b"key", b"score"]},
        {"step": "MATCH C1 and C2 after first server  swap", "node":data, "command": "g match(C2,C1)", "expects": []},
        {"step": "MATCH S1 and S2 after first server swap", "node":data, "command": "g match(S1,S2)", "expects": [[b'subject', b'S1', b'length', b'2', b'object', b'S2', b'path', [[b'S1', b'0'], [b'I_OF:S1:C2', b'0.5'], [b'C2', b'0.5'], [b'I_OF:S2:C2', b'0.5'], [b'S2', b'0.5']]]]},
        {"step": "MATCH S1 and S3 after first server swap", "node":data, "command": "g match(S1,S3)", "expects": []},
        {"step": "MATCH S3 and S4 after first server swap", "node":data, "command": "g match(S3,S4)", "expects": [[b'subject', b'S3', b'length', b'2', b'object', b'S4', b'path', [[b'S3', b'0'], [b'I_OF:S3:C1', b'0.5'], [b'C1', b'0.5'], [b'I_FOR:C1:S4', b'0.5'], [b'S4', b'0.5']]]]},
        {"step": "SWAP servers S2 and S4", "node":data, "command": "g swapkeys(S2,S4)", "expects": [b'No results!']},
        {"step": "FUSE C1 after second server  swap", "node":data, "command": "g V(C1).fuseout(I_OF)", "expects": [[b'value', [b'subject', b'C1', b'object', b'C1', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S1:C1', b'edge', b'I_FOR:C1:S1']], [b'value', [b'subject', b'C1', b'object', b'C1', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S2:C1', b'edge', b'I_FOR:C1:S2']]], "strip":[b"key", b"score"]},
        {"step": "FUSE C2 after second server  swap", "node":data, "command": "g V(C2).fuseout(I_OF)", "expects": [[b'value', [b'subject', b'C2', b'object', b'C2', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S4:C2', b'edge', b'I_FOR:C2:S4']], [b'value', [b'subject', b'C2', b'object', b'C2', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S3:C2', b'edge', b'I_FOR:C2:S3']]], "strip":[b"key", b"score"]},
        {"step": "MATCH C1 and C2 after second server  swap", "node":data, "command": "g match(C2,C1)", "expects": []},
        {"step": "MATCH S1 and S2 after second server swap", "node":data, "command": "g match(S1,S2)", "expects": [[b'subject', b'S1', b'length', b'2', b'object', b'S2', b'path', [[b'S1', b'0'], [b'I_OF:S1:C2', b'0.5'], [b'C2', b'0.5'], [b'I_OF:S2:C2', b'0.5'], [b'S2', b'0.5']]]]},
        {"step": "MATCH S1 and S3 after second server swap", "node":data, "command": "g match(S1,S3)", "expects": []},
        {"step": "MATCH S3 and S4 after second server swap", "node":data, "command": "g match(S3,S4)", "expects": [[[b'subject', b'S3', b'length', b'2', b'object', b'S4', b'path', [[b'S3', b'0'], [b'I_OF:S3:C1', b'0.5'], [b'C1', b'0.5'], [b'I_FOR:C1:S4', b'0.5'], [b'S4', b'0.5']]]],[[b'subject', b'S3', b'length', b'2', b'object', b'S4', b'path', [[b'S3', b'0'], [b'I_FOR:C1:S3', b'0.5'], [b'C1', b'0.5'], [b'I_OF:S4:C1', b'0.5'], [b'S4', b'0.5']]]]]},
    ]
    matcher.play(scenario)


def SD_950_Swap_keys(cluster_id, controller, data, index):
    matcher = Matcher("SD_950_Swap_keys[graph]")

    scenario = [
        {"step": "ADD TEST DATA C1xS1", "node": data, "command": "g addv(C1,cluster).addv(S1,server).addv(S2,server).reset.predicate(I_FOR,I_OF).subject(C1).object(S1).reset.predicate(I_FOR,I_OF).subject(C1).object(S2)", "expects": [[b'key', b'I_FOR:C1:S1', b'score', b'1', b'value', [b'type', b'I_FOR', b'half', b'I_OF:S1:C1', b'edge', b'I_FOR:C1:S1']], [b'key', b'I_OF:S1:C1', b'score', b'1', b'value', [b'type', b'I_OF', b'half', b'I_OF:S1:C1', b'edge', b'I_FOR:C1:S1']]]},
        {"step": "ADD TEST DATA C2xS3", "node": data, "command": "g addv(C2,cluster).addv(S3,server).addv(S4,server).reset.predicate(I_FOR,I_OF).subject(C2).object(S3).reset.predicate(I_FOR,I_OF).subject(C2).object(S4)", "expects": [[b'key', b'I_FOR:C2:S3', b'score', b'1', b'value', [b'type', b'I_FOR', b'half', b'I_OF:S3:C2', b'edge', b'I_FOR:C2:S3']], [b'key', b'I_OF:S3:C2', b'score', b'1', b'value', [b'type', b'I_OF', b'half', b'I_OF:S3:C2', b'edge', b'I_FOR:C2:S3']]]},

        {"step": "MATCH S1 and S2 before first server  swap", "node":data, "command": "g match(S1,S2)", "expects": [[b'subject', b'S1', b'length', b'2', b'object', b'S2', b'path', [[b'S1', b'0'], [b'I_FOR:C1:S1', b'0.5'], [b'C1', b'0.5'], [b'I_OF:S2:C1', b'0.5'], [b'S2', b'0.5']]]]},
        {"step": "MATCH S3 and S4 before first server  swap", "node":data, "command": "g match(S3,S4)", "expects": [[[b'subject', b'S3', b'length', b'2', b'object', b'S4', b'path', [[b'S3', b'0'], [b'I_OF:S3:C2', b'0.5'], [b'C2', b'0.5'], [b'I_FOR:C2:S4', b'0.5'], [b'S4', b'0.5']]]], [[b'subject', b'S3', b'length', b'2', b'object', b'S4', b'path', [[b'S3', b'0'], [b'I_FOR:C2:S3', b'0.5'], [b'C2', b'0.5'], [b'I_OF:S4:C2', b'0.5'], [b'S4', b'0.5']]]]]},
        {"step": "FUSE C1 before first server  swap", "node":data, "command": "g V(C1).fuseout(I_OF)", "expects": [[b'value', [b'subject', b'C1', b'object', b'C1', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S1:C1', b'edge', b'I_FOR:C1:S1']], [b'value', [b'subject', b'C1', b'object', b'C1', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S2:C1', b'edge', b'I_FOR:C1:S2']]], "strip":[b"key", b"score"]},
        {"step": "FUSE C2 before first server  swap", "node":data, "command": "g V(C2).fuseout(I_OF)", "expects": [[b'value', [b'subject', b'C2', b'object', b'C2', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S4:C2', b'edge', b'I_FOR:C2:S4']], [b'value', [b'subject', b'C2', b'object', b'C2', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S3:C2', b'edge', b'I_FOR:C2:S3']]], "strip":[b"key", b"score"]},
        {"step": "SWAP servers S1 and S3", "node":data, "command": "g swapkeys(S1,S3)", "expects": [b'No results!']},
        {"step": "FUSE C1 after first server  swap", "node":data, "command": "g V(C1).fuseout(I_OF)", "expects": [[b'value', [b'subject', b'C1', b'object', b'C1', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S1:C1', b'edge', b'I_FOR:C1:S1']], [b'value', [b'subject', b'C1', b'object', b'C1', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S2:C1', b'edge', b'I_FOR:C1:S2']]], "strip":[b"key", b"score"]},
        {"step": "FUSE C2 after first server  swap", "node":data, "command": "g V(C2).fuseout(I_OF)", "expects": [[b'value', [b'subject', b'C2', b'object', b'C2', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S4:C2', b'edge', b'I_FOR:C2:S4']], [b'value', [b'subject', b'C2', b'object', b'C2', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S3:C2', b'edge', b'I_FOR:C2:S3']]], "strip":[b"key", b"score"]},
        {"step": "MATCH C1 and C2 after first server  swap", "node":data, "command": "g match(C2,C1)", "expects": []},
        {"step": "MATCH S1 and S2 after first server swap", "node":data, "command": "g match(S1,S2)", "expects": [[b'subject', b'S1', b'length', b'2', b'object', b'S2', b'path', [[b'S1', b'0'], [b'I_OF:S1:C2', b'0.5'], [b'C2', b'0.5'], [b'I_OF:S2:C2', b'0.5'], [b'S2', b'0.5']]]]},
        {"step": "MATCH S1 and S3 after first server swap", "node":data, "command": "g match(S1,S3)", "expects": []},
        {"step": "MATCH S3 and S4 after first server swap", "node":data, "command": "g match(S3,S4)", "expects": [[b'subject', b'S3', b'length', b'2', b'object', b'S4', b'path', [[b'S3', b'0'], [b'I_OF:S3:C1', b'0.5'], [b'C1', b'0.5'], [b'I_FOR:C1:S4', b'0.5'], [b'S4', b'0.5']]]]},
        {"step": "SWAP servers S2 and S4", "node":data, "command": "g swapkeys(S2,S4)", "expects": [b'No results!']},
        {"step": "FUSE C1 after second server  swap", "node":data, "command": "g V(C1).fuseout(I_OF)", "expects": [[b'value', [b'subject', b'C1', b'object', b'C1', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S1:C1', b'edge', b'I_FOR:C1:S1']], [b'value', [b'subject', b'C1', b'object', b'C1', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S2:C1', b'edge', b'I_FOR:C1:S2']]], "strip":[b"key", b"score"]},
        {"step": "FUSE C2 after second server  swap", "node":data, "command": "g V(C2).fuseout(I_OF)", "expects": [[b'value', [b'subject', b'C2', b'object', b'C2', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S4:C2', b'edge', b'I_FOR:C2:S4']], [b'value', [b'subject', b'C2', b'object', b'C2', b'predicate', b'', b'type', b'I_OF', b'half', b'I_OF:S3:C2', b'edge', b'I_FOR:C2:S3']]], "strip":[b"key", b"score"]},
        {"step": "MATCH C1 and C2 after second server  swap", "node":data, "command": "g match(C2,C1)", "expects": []},
        {"step": "MATCH S1 and S2 after second server swap", "node":data, "command": "g match(S1,S2)", "expects": [[b'subject', b'S1', b'length', b'2', b'object', b'S2', b'path', [[b'S1', b'0'], [b'I_OF:S1:C2', b'0.5'], [b'C2', b'0.5'], [b'I_OF:S2:C2', b'0.5'], [b'S2', b'0.5']]]]},
        {"step": "MATCH S1 and S3 after second server swap", "node":data, "command": "g match(S1,S3)", "expects": []},
        {"step": "MATCH S3 and S4 after second server swap", "node":data, "command": "g match(S3,S4)", "expects": [[[b'subject', b'S3', b'length', b'2', b'object', b'S4', b'path', [[b'S3', b'0'], [b'I_OF:S3:C1', b'0.5'], [b'C1', b'0.5'], [b'I_FOR:C1:S4', b'0.5'], [b'S4', b'0.5']]]],[[b'subject', b'S3', b'length', b'2', b'object', b'S4', b'path', [[b'S3', b'0'], [b'I_FOR:C1:S3', b'0.5'], [b'C1', b'0.5'], [b'I_OF:S4:C1', b'0.5'], [b'S4', b'0.5']]]]]},
    ]
    matcher.play(scenario)


