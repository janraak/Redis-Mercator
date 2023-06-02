from __future__ import print_function
import redis
import pdb
import traceback
import time
import sys
import json
from os import system
from os.path import abspath
import os.path
from pathlib import Path
import subprocess
import sys
import os
import inspect
from inspect import getmembers, isfunction

global redis_path
global dataset1
global dataset1ref

import glob, importlib, os, pathlib, sys
def load_scenarios():
    all_methods = []
    dir_path = os.path.dirname(os.path.realpath(__file__))
    MODULE_DIR = "{}/{}".format(dir_path, "scenarios")
    # The directory containing your modules needs to be on the search path.
    sys.path.append(MODULE_DIR)

    # Get the stem names (file name, without directory and '.py') of any 
    # python files in your directory, load each module by name and run
    # the required function.
    py_files = glob.glob(os.path.join(MODULE_DIR, '*.py'))
    for py_file in py_files:
        module_name = pathlib.Path(py_file).stem
        module = importlib.import_module(module_name)
        callables = getmembers(module, isfunction)
        for t in callables:
            all_methods.append(t)
            
    return all_methods


def which_mercator_modules_has_been_loaded(redis_client):
    module_tags = {}
    data = redis_client.execute_command("MODULE LIST")
    for m in data:
        print(m)
        if m[b'name'].decode('utf-8') == "rxGraphdb":
            module_tags["rxGraphdb"] = True
        if m[b'name'].decode('utf-8') == "rxMercator":
            module_tags["rxMercator"] = True
        if m[b'name'].decode('utf-8') == "rxIndexStore":
            module_tags["rxIndexStore"] = True
        if m[b'name'].decode('utf-8') == "rxIndexer":
            module_tags["rxIndexer"] = True
        if m[b'name'].decode('utf-8') == "RXQUERY":
            module_tags["rxQuery"] = True
        if m[b'name'].decode('utf-8') == "rxRule":
            module_tags["rxRule"] = True
    return module_tags


def get_redis_path(redis_client):
    info = redis_client.info("SERVER")
    segments = info["executable"].split('/')
    path = '/'.join(segments[0:len(segments)-2])
    return path


def prepare_controller():
    redis_client = redis.StrictRedis('localhost', 6380, 0)
    redis_path = get_redis_path(redis_client)

    module_config = which_mercator_modules_has_been_loaded(redis_client)
    if not "rxMercator" in module_config:
        r = redis_client.execute_command(
            "MODULE LOAD {}/extensions/src/rxMercator.so".format(redis_path))
        print("rxMercator loaded")
        r = redis_client.execute_command(
            "mercator.add.server localhost 127.0.0.1 6GB BASE 6381 2")
        print("rxMercator server added")
    return redis_client


def connect_to_redis(node):
    redis_client = redis.StrictRedis(node["ip"], node["port"], 0)
    return redis_client


def create_cluster(redis_client):
    cluster_id = redis_client.execute_command("mercator.create.cluster", "::1")
    redis_client.execute_command("mercator.start.cluster", cluster_id)
    cluster_info = json.loads(redis_client.execute_command(
        "mercator.info.cluster", cluster_id).decode('utf-8'))
    info = {"cluster_id": cluster_id}
    for node in cluster_info["nodes"]:
        if node["role"] == "data":
            info["data"] = node
        elif node["role"] == "index":
            info["index"] = node
    return info


def download_testdata(redis_client, fname):
    redis_client.execute_command("g.wget", "https://roxoft.dev/assets/{}".format(fname))

ignores = 'abspath'
def run_test(scenario, cluster_id, controller, data_node, index_node):
    fname = scenario[0]
    if type(fname) != str:
        print(fname)
        pdb.set_trace()
        return
    if fname in ignores:
        return
    fnc = scenario[1]
    print("{} ... ".format(fname), end="")    
    controller.execute_command("mercator.flush.cluster {}".format(cluster_id))
    data_node.flushall()
    index_node.flushall()
    try:
        # fnc = globals()[scenario]
        fnc(cluster_id, controller, data_node, index_node)   
        print("succeded")    
    except Exception as ex:
        print(" failed, error: {}".format(ex.message))
        pdb.set_trace()

def my_import(name):    
    components = name.split('.')
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

def main():
    scenarios = load_scenarios();
    # scenarios = [method_name for method_name in globals()
    #               if "scenario" in method_name]

    controller = prepare_controller()
    cluster_info = create_cluster(controller)
    data_node = connect_to_redis(cluster_info["data"])
    index_node = connect_to_redis(cluster_info["index"])

    dataset1 = download_testdata(data_node, "dataset1.txt")
    cluster_id = cluster_info["cluster_id"].decode('utf-8')
    for n in range(0,10):
        print("------ run {}".format(n))
        for scenario in scenarios:
            # system('clear')
            run_test(scenario, cluster_id, controller,
                                data_node, index_node)

    index_node.close()
    data_node.close()
    controller.close()


if __name__ == "__main__":
    has_exception = False
    nfails = 0
    main()
