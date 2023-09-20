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
global filter
filter = {"folder":"scenarios", "runs":2}

import glob, importlib, os, pathlib, sys
parameter_names = ["testset", "include", "exclude", "runs"]
def parse_arguments(argv):
    n = 0
    folder = "scenarios"
    includes = []
    excludes = []
    runs = 10
    if len(argv) > 1:
        #[testset %folder] [include %filename, ...] [exclude %filename, ...]
        while n < len(argv):
            if argv[n] =="testset":
                folder = argv[n + 1]
                n += 1
            elif argv[n] =="runs":
                runs = int(argv[n + 1])
                n += 1
            elif argv[n] =="include":
                n += 1
                while n < len(argv) and not argv[n] in parameter_names:
                    includes.append(argv[n])
                    n += 1
            elif argv[n] == "exclude":
                n += 1
                while n < len(argv) and not argv[n] in parameter_names:
                    excludes.append(argv[n])
                    n += 1
            n += 1
    return {"folder":folder, "includes":includes, "excludes":excludes, "runs":runs}

def load_scenarios(argv):
    filter = parse_arguments(argv)
    all_files = []
    dir_path = os.path.dirname(os.path.realpath(__file__))
    MODULE_DIR = "{}/{}".format(dir_path, filter["folder"])
    # #pdb.set_trace()
    # The directory containing your modules needs to be on the search path.
    sys.path.append(MODULE_DIR)

    # Get the stem names (file name, without directory and '.py') of any 
    # python files in your directory, load each module by name and run
    # the required function.
    py_files = glob.glob(os.path.join(MODULE_DIR, '*.py'))
    for py_file in py_files:
        if type(filter["excludes"]) == type(list):
            for e in filter["excludes"]:
                if e in py_file:
                    continue
        all_methods = []
        module_name = pathlib.Path(py_file).stem
        module = importlib.import_module(module_name)
        callables = getmembers(module, isfunction)
        for t in callables:
            if str(inspect.signature(t[1])) != "(cluster_id, controller, data, index)":
                print("ignored: {} signature: {}".format(t[0], inspect.signature(t[1])))
                # #pdb.set_trace()
                continue
            if type(filter["includes"]) == type(list):
                for i in filter["includes"]:
                    if i in t[0]:
                        all_methods.append(t)
            else:
                all_methods.append(t)
        all_files.append(all_methods)
            
    return all_files


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

def find_local_ip():
    for l in os.popen("ip -o -4 a").read().split("\n"):
        xface = l.split(' ')
        if xface[1] != 'lo':
            for c in range(2, len(xface)):
                if(xface[c] == 'inet'):
                    return xface[c+1].split('/')[0]
    return '127.0.0.1'

def prepare_controller():
    ip = find_local_ip()
    redis_client = redis.StrictRedis(ip, 6380, 0)
    redis_path = get_redis_path(redis_client)
    #pdb.set_trace()
    module_config = which_mercator_modules_has_been_loaded(redis_client)
    if not "rxMercator" in module_config:
        r = redis_client.execute_command(
            "MODULE LOAD {}/extensions/src/rxQuery.so".format(redis_path))
        print("rxMercator loaded")
        r = redis_client.execute_command(
            "MODULE LOAD {}/extensions/src/rxMercator.so".format(redis_path))
        print("rxMercator loaded")
        r = redis_client.execute_command(
            "mercator.add.server localhost {} 6GB BASE 6381 2".format(ip))
        print("rxMercator server added")
    return redis_client


def connect_to_redis(node):
    redis_client = redis.StrictRedis(node["ip"], node["port"], 0)
    #pdb.set_trace()
    return redis_client


def create_cluster(redis_client):
    cluster_id = redis_client.execute_command("mercator.create.cluster", "SYS_TESTS")
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
    # redis_client.execute_command("g.wget", "https://roxoft.dev/assets/{}".format(fname))
    pass

ignores = 'abspath'
def run_test(scenario, cluster_id, controller, data_node, index_node, flushall = True):
    fname = scenario[0]
    if type(fname) != str:
        print(fname)
        return
    if fname in ignores:
        return
    fnc = scenario[1]
    print("{} ... ".format(fname), end="")    
    if flushall:

        controller.execute_command("mercator.flush.cluster {}".format(cluster_id))
        data_node.flushall()
        index_node.flushall()
        print(" database flushed ", end="")    
    try:
        # fnc = globals()[scenario]
        fnc(cluster_id, controller, data_node, index_node)   
        print("succeded")    
    except Exception as ex:
        print(" failed, error: {}".format(ex))

def my_import(name):    
    components = name.split('.')
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

def main(argv):
    scenarios = load_scenarios(argv);
    # scenarios = [method_name for method_name in globals()
    #               if "scenario" in method_name]

    controller = prepare_controller()
    cluster_info = create_cluster(controller)
    #pdb.set_trace()
    data_node = connect_to_redis(cluster_info["data"])
    index_node = connect_to_redis(cluster_info["index"])

    dataset1 = download_testdata(data_node, "dataset1.txt")
    cluster_id = cluster_info["cluster_id"].decode('utf-8')
    for n in range(0,filter["runs"]):
        print("------ run {}".format(n))
        flushall = True
        for file in scenarios:
            for scenario in file:
                print("setup: {}", scenario[0])
                if "setup" in scenario[0]:
                    print("execute setup: {}", scenario[0])
                    run_test(scenario, cluster_id, controller,
                                        data_node, index_node, False)
                    flushall = False
            for scenario in file:
                print("test: {}", scenario[0])
                if not ("setup" in scenario[0]):
                    print("execute test: {}", scenario[0])
                    run_test(scenario, cluster_id, controller,
                                        data_node, index_node, flushall)

    index_node.close()
    data_node.close()
    controller.close()


if __name__ == "__main__":
    has_exception = False
    nfails = 0
    main(sys.argv)
else:
    print("Must be run using python3\n\npython3 _runner.py [testset %folder] [include %filename, ...] [exclude %filename, ...]")