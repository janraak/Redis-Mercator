import redis
import pdb
import traceback
import time
import sys

from os.path import abspath
import os.path
from pathlib import Path


def execute(n,client, cmd):
    print("{}: {}".format(n,cmd))
    r = client.execute_command(cmd)
    # print("{}= {}".format(n,r))
    return r

def simple_indexing_demo_verification(cluster_id, controller, data, index):
    repeat_length = 1
    query_repeat_length = 1
    n = 0

    print("TEST SIMPLE STRING")
    execute(n,data,"flushall")
    index.execute_command("flushall")
    for j in range(0,repeat_length):
        data.delete("UN")
        data.set("UN","United Nations")
        execute(n,data,"rxindex wait")
        index.keys("*")
        for j in range(0,query_repeat_length):
            execute(n,data,"RXQUERY united nations")
    execute(n,data,"GET UN")
    execute(n,data,"RXQUERY united | nations")
    execute(n,data,"RXQUERY united & nations")

    print("TEST TEXT STRING")
    execute(n,data,"flushall")
    index.execute_command("flushall")
    for j in range(0,repeat_length):
        data.delete("UN")
        data.set("UN", "United Nations. HQ: New York. Established: 1945.")
        execute(n,data,"rxindex wait")
        index.keys("*")
        for j in range(0,query_repeat_length):
            execute(n,data,"RXQUERY united nations")
    execute(n,data,"rxindex wait")
    execute(n,data,"GET UN")
    execute(n,data,"RXQUERY united & nations")
    execute(n,data,"RXQUERY new | york")
    execute(n,data,"RXQUERY new & york")
    execute(n,data,"RXQUERY united | nations | 1945")
    execute(n,data,"RXQUERY united & nations & 1945")

    print("TEST JSON STRING")
    execute(n,data,"flushall")
    index.execute_command("flushall")
    for j in range(0,repeat_length):
        data.delete("UN")
        json ="{\"Organisation\": \"United Nations\", \"HQ\": \"New York\", \"Established\": \"1945\"}"
        print(json)
        data.set("UN", json)
        execute(n,data,"rxindex wait")
        index.keys("*")
        for j in range(0,query_repeat_length):
            execute(n,data,"RXQUERY united nations")
    execute(n,data,"GET UN")
    execute(n,data,"RXQUERY united | nations")
    execute(n,data,"RXQUERY united & nations")
    execute(n,data,"RXQUERY new | york")
    execute(n,data,"RXQUERY new & york")
    execute(n,data,"RXQUERY united | nations | 1945")
    execute(n,data,"RXQUERY united & nations & 1945")

    print("TEST SIMPLE HASH")
    execute(n,data,"flushall")
    index.execute_command("flushall")
    avp = {"name":"United Nations", "HQ":"New York", "Established":1945}
    for j in range(0,repeat_length):
        data.delete("UN")
        data.hmset('UN', avp)
        execute(n,data,"rxindex wait")
        index.keys("*")
        for j in range(0,query_repeat_length):
            execute(n,data,"RXQUERY united nations")
    execute(n,data,"HGETALL UN")
    execute(n,data,"rxindex wait")
    execute(n,data,"RXQUERY united | nations")
    execute(n,data,"RXQUERY united & nations")
    execute(n,data,"RXQUERY new | york")
    execute(n,data,"RXQUERY new & york")
    execute(n,data,"RXQUERY united | nations | 1945")

    execute(n,data,"RXQUERY united & nations & 1945")
