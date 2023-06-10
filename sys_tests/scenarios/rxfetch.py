import redis
import pdb
import sys

from os.path import abspath
import os.path
from pathlib import Path

def verify_members(standing, assertion):
    failed = False
    if len(standing) != len(assertion):
        failed = True
    else:
        for value in assertion:
            if not value in standing:
                failed = True
                print("missing element {}".format(value))
    if failed:
        print("Mismatch, expected {} element, found {}\nE: {}\nF: {}".format(len(assertion),len(standing),assertion,standing))

def verify_keys(index, assertion):
    members = index.execute_command("keys _*")
    client_keys = []
    index_keys = []
    client_members = []
    for k in members:
        if k.decode().startswith("_ox_:"):
            client_keys.append(k.decode())
            client_members.append(k.decode().replace("_ox_:","")+"\tH")
        elif k.decode().startswith("_zx_:"):
            index_keys.append(k.decode())
    for ck in client_keys:
        members = index.execute_command("smembers " + ck)
        verify_members(members, assertion)
    for ik in index_keys:
        members = index.execute_command("zrange " + ik + " 0 100")
        decoded_members = []
        for m in members:
            decoded_members.append(m.decode())
        for cm in client_members:
            if not cm in decoded_members:
                print("missing link:{}".format(cm))

def rxIndexStore_test(cluster_id, controller, data, index):
    index.execute_command("rxAdd NL H kleur rood 0.333")
    index.execute_command("rxAdd NL H kleur wit 0.333")
    index.execute_command("rxAdd NL H kleur blauw 0.333")
    index.execute_command("rxAdd NL H kleur blaur 0.333")
    verify_keys(index, [b'_zx_:blauw:kleur', b'_zx_:blaur:kleur', b'_zx_:rood:kleur', b'_zx_:wit:kleur'])

    index.execute_command("rxBegin NL")
    index.execute_command("rxAdd NL H kleur rood 0.333")
    index.execute_command("rxAdd NL H kleur wit 0.333")
    index.execute_command("rxAdd NL H kleur blauw 0.333")
    index.execute_command("rxCommit NL")
    verify_keys(index, [b'_zx_:rood:kleur', b'_zx_:blauw:kleur', b'_zx_:wit:kleur'])

    index.execute_command("rxBegin NL")
    index.execute_command("rxCommit NL")
    verify_keys(index, [])

    index.execute_command("rxBegin NL")
    index.execute_command("rxAdd NL H kleur rood 0.333")
    index.execute_command("rxAdd NL H kleur wit 0.333")
    index.execute_command("rxAdd NL H kleur blauw 0.333")
    index.execute_command("rxCommit NL")
    verify_keys(index, [b'_zx_:rood:kleur', b'_zx_:blauw:kleur', b'_zx_:wit:kleur'])

    index.execute_command("rxBegin NL")
    index.execute_command("rxRollback NL")
    verify_keys(index, [b'_zx_:rood:kleur', b'_zx_:blauw:kleur', b'_zx_:wit:kleur'])
