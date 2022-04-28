import redis
import pdb
import traceback
import dataset_family
import time
import sys

from os.path import abspath
import os.path
from pathlib import Path

def check_server(must_flush):

    print ('Number of arguments: {} arguments.'.format(len(sys.argv))) 
    print ('Argument List: {}'.format( str(sys.argv)))
    filename = abspath(sys.argv[0])
    print(filename)
    path = Path(sys.argv[0])
    modulePath = path.parent.parent.joinpath("src").absolute()
    print(modulePath)

    redis_client = redis.StrictRedis('192.168.1.180', 6379, 0)

    while True:
        data =  redis_client.execute_command("info Persistence").decode('utf-8')
        if "loading:0" in data:
            break
        data =  redis_client.execute_command("info Persistence").decode('utf-8')

    data = redis_client.execute_command("MODULE LIST")
    fetcher_loaded = False
    for m in data:
        if m[b'name'].decode('utf-8') == "rxFetch": fetcher_loaded= True
    if not fetcher_loaded:
        redis_client.execute_command("MODULE LOAD {}/rxFetch.so ".format(modulePath))

    return redis_client

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

def verify_keys(redis_client, assertion):
    members = redis_client.execute_command("keys _*")
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
        members = redis_client.execute_command("smembers " + ck)
        verify_members(members, assertion)
    for ik in index_keys:
        members = redis_client.execute_command("zrange " + ik + " 0 100")
        decoded_members = []
        for m in members:
            decoded_members.append(m.decode())
        for cm in client_members:
            if not cm in decoded_members:
                print("missing link:{}".format(cm))

def rxfetch_test(redis_client):
    redis_client.execute_command("rxAdd NL H kleur rood 0.333")
    redis_client.execute_command("rxAdd NL H kleur wit 0.333")
    redis_client.execute_command("rxAdd NL H kleur blauw 0.333")
    redis_client.execute_command("rxAdd NL H kleur blaur 0.333")
    verify_keys(redis_client, [b'_zx_:blauw:kleur', b'_zx_:blaur:kleur', b'_zx_:rood:kleur', b'_zx_:wit:kleur'])

    redis_client.execute_command("rxBegin NL")
    redis_client.execute_command("rxAdd NL H kleur rood 0.333")
    redis_client.execute_command("rxAdd NL H kleur wit 0.333")
    redis_client.execute_command("rxAdd NL H kleur blauw 0.333")
    redis_client.execute_command("rxCommit NL")
    verify_keys(redis_client, [b'_zx_:rood:kleur', b'_zx_:blauw:kleur', b'_zx_:wit:kleur'])

    redis_client.execute_command("rxBegin NL")
    redis_client.execute_command("rxCommit NL")
    verify_keys(redis_client, [])

    redis_client.execute_command("rxBegin NL")
    redis_client.execute_command("rxAdd NL H kleur rood 0.333")
    redis_client.execute_command("rxAdd NL H kleur wit 0.333")
    redis_client.execute_command("rxAdd NL H kleur blauw 0.333")
    redis_client.execute_command("rxCommit NL")
    verify_keys(redis_client, [b'_zx_:rood:kleur', b'_zx_:blauw:kleur', b'_zx_:wit:kleur'])

    redis_client.execute_command("rxBegin NL")
    redis_client.execute_command("rxRollback NL")
    verify_keys(redis_client, [b'_zx_:rood:kleur', b'_zx_:blauw:kleur', b'_zx_:wit:kleur'])


def main(must_flush = False):
    redis_client = check_server(must_flush)
    rxfetch_test(redis_client)
    redis_client.close()

if __name__ == "__main__":
    main(must_flush=True)
