from __future__ import print_function
from colorama import Fore, Back, Style
import pdb
import hashlib
import ast
import traceback
import os.path
import os
import asyncio
import logging
import threading
import time

class ReconnectException(BaseException):
    def __init__(self, ex) -> None:
        pass

    def __init__(self) -> None:
        pass


match_calls = 0


def find_local_ip():
    for l in os.popen("ip -o -4 a").read().split("\n"):
        xface = l.split(" ")
        if xface[1] != "lo":
            for c in range(2, len(xface)):
                if xface[c] == "inet":
                    return xface[c + 1].split("/")[0]
    return "127.0.0.1"


def left(s, pos):
    return s[:pos]


def right(s, pos):
    return s[pos:]


def reset_match_calls():
    global match_calls
    match_calls = 0


def get_match_calls():
    global match_calls
    return match_calls


def list_to_hash(h, l):
    if l is None:
        return h
    if isinstance(l, dict):
        for key in l:
            h.update(bytes(key))
            h.update(bytes(l[key]))
        return h
    elif not isinstance(l, list):
        try:
            if isinstance(l, str):
                h.update(bytes(l, "utf-8"))
            elif isinstance(l, bytes):
                h.update(l)
            else:
                h.update(bytes(l))
        except Exception as ex:
            print(" add {} `{}` to hash, error: {}".format(type(l), l, ex))
            traceback.print_exc()
            pass
        return h
    for e in l:
        if isinstance(e, dict):
            for key in e:
                h.update(bytes(key))
                h.update(bytes(e[key]))
        elif isinstance(e, list):
            list_to_hash(h, e)
        else:
            h.update(bytes(e))
    return h


def flatten_list(o, l):
    if l is None:
        return o
    if isinstance(l, dict):
        for key in l:
            o.append(key)
            flatten_list(o, l[key])
        return o
    if not isinstance(l, list):
        o.append(l)
        return o
    for e in l:
        if isinstance(e, dict):
            flatten_list(o, e)
        elif isinstance(e, list):
            flatten_list(o, e)
        else:
            o.append(e)
    return o


def match_hashed(step, outcome, expected):
    if isinstance(outcome, dict):
        outcome = flatten_list([], outcome)
    if isinstance(expected, dict):
        expected = flatten_list([], expected)
    if isinstance(expected, str):
        try:
            expected = ast.literal_eval(expected)
        except:
            pass
    global match_calls
    match_calls += 1
    print(Style.RESET_ALL)
    try:
        rHasher = hashlib.sha1()
        list_to_hash(rHasher, outcome)
        eHasher = hashlib.sha1()
        list_to_hash(eHasher, expected)
        if rHasher.digest() == eHasher.digest():
            print("STEP:   {}{}".format(Fore.GREEN, step))
            return 1
        fo = flatten_list([], outcome)
        fe = flatten_list([], expected)
        if match_sets(step, fo, fe) == 1:
            return 1
    except Exception as ex:
        print(" failed, error: {}".format(ex))
        traceback.print_exc()
    return 0


def match_sets(step, outcome, expected):
    if isinstance(expected, str):
        expected = ast.literal_eval(expected)
    if isinstance(outcome, dict):
        outcome = flatten_list([], outcome)
        pdb.set_trace()
    if expected is dict:
        expected = flatten_list([], expected)
        pdb.set_trace()

    global match_calls
    match_calls += 1
    print(Style.RESET_ALL)
    try:
        if not isinstance(outcome, set):
            rSet = set(flatten_list([], outcome))
        else:
            rSet = outcome
        if not isinstance(expected, set):
            eSet = set(flatten_list([], expected))
        else:
            eSet = expected
        diff = rSet.symmetric_difference(eSet)
        if len(diff) == 0:
            print("STEP:   {}{}".format(Fore.GREEN, step))
            return 1
        print("{}failed, diff: {}".format(Fore.YELLOW + Back.RED, diff))
        print(diff)
        return match_strings(step, outcome, expected)
    except Exception as ex:
        print(
            f" failed, error: {ex}\nR: {type(outcome)}  {outcome}\nE:{type(expected)}  {expected}"
        )
        traceback.print_exc()
    return 0


def match_strings_with_expected(step, outcome, expected):
    if not isinstance(outcome, str):
        outcome = f"{outcome}"
    if not isinstance(expected, str):
        expected = f"{expected}"
    if expected[0] == "*":
        if (expected[1:]) in outcome:
            return 1
    elif (
        len(expected) > 128
        and left(outcome, 64) == left(expected, 64)
        and right(outcome, 64) == right(expected, 64)
    ):
        return 1
    elif f"{outcome}" == expected:
        return 1
    return 0


def match_strings(step, outcome, expected):
    if isinstance(expected, list):
        for e in expected:
            m = match_strings_with_expected(step, outcome, e)
            if m == 1:
                print(f"{Style.RESET_ALL}STEP:   {Fore.GREEN}{step}")
                return m
    else:
        m = match_strings_with_expected(step, outcome, expected)
        if m == 1:
            print(f"{Style.RESET_ALL}STEP:   {Fore.GREEN}{step}")
            return m

    print(
        f"{Style.RESET_ALL}\nSTEP:   {Fore.BLUE}{step}\nFAIL:   {Fore.RED}{outcome}\nEXPECT: {Fore.CYAN}{expected}\n"
    )
    return 0


def match(step, outcome, expected):
    return match_hashed(step, outcome, expected)


def stripField(outcome, field):
    if not isinstance(outcome, list):
        return outcome
    result = []
    for r in outcome:
        if isinstance(field, list):
            for f in field:
                if f in r:
                    pi = r.index(f)
                    if pi is not None:
                        r = r[:pi] + r[pi + 2 :]
        else:
            pi = r.index(field)
            if pi is not None:
                r = r[:pi] + r[pi + 2 :]
        result.append(r)

    return result


def filterRows(outcome, field, values):
    if not isinstance(outcome, list):
        return outcome
    result = []
    for r in outcome:
        pi = r.index(field)
        if pi is not None and r[pi + 1] in values:
            result.append(r)

    return result


def HeaderOnly(outcome):
    return stripField(outcome, b"path")

def play_task(ctl, repeats, scenario):
    for n in range(0,repeats):
        ctl.play(scenario, True)

class Matcher:
    def __init__(self, test_name) -> None:
        self.succes_tally = 0
        self.match_calls = 0
        self.test_name = test_name

    def by_hashed(self, step, outcome, expected):
        self.match_calls += 1
        matched = match_hashed(step, outcome, expected)
        self.succes_tally += matched
        return matched

    def by_sets(self, step, outcome, expected):
        self.match_calls += 1
        matched = match_sets(step, outcome, expected)
        self.succes_tally += matched
        return matched

    def by_strings(self, step, outcome, expected):
        self.match_calls += 1
        matched = match_strings(step, outcome, expected)
        self.succes_tally += matched
        return matched

    def finalize(self):
        if self.succes_tally != self.match_calls:
            raise AssertionError(
                f"FAILED: {self.test_name}, {self.match_calls - self.succes_tally} of {self.match_calls} steps failed"
            )

    
    def play(self, scenario, no_finalize = False):
        tasks = []
        for facet in scenario:
            if "debug" in facet:
                pdb.set_trace()
            if "parallel" in facet:
                parallel = facet["parallel"]
                repeats = parallel["repeats"]
                name = parallel["name"]
                task = threading.Thread(target=play_task, args=(self, repeats, parallel["steps"],))
                task.start()
                # task = asyncio.create_task(play_task(self, repeats, parallel["steps"]))
                tasks.append(task)
                print(f'\033[1;{1 + len(tasks) * 40}H{Style.RESET_ALL}{Fore.YELLOW}{Back.LIGHTGREEN_EX} Task started for: {name}\033[1E{Style.RESET_ALL}')
                continue
                
            if "pace" in facet:    
                pace = facet["pace"]
                time.sleep(pace)
                
            strip_set = facet["strip"] if "strip" in facet else None
            msg = f'#{1 + self.match_calls} {facet["step"]}'
            node = facet["node"]
            command = facet["command"]
            expects = facet["expects"]
            if isinstance(expects, str):
                self.by_strings(msg, node.execute_command(command), expects)
            elif isinstance(strip_set, list):
                self.by_sets(
                    msg,
                    stripField(node.execute_command(command), strip_set),
                    stripField(expects, strip_set),
                )
            else:
                self.by_sets(msg, node.execute_command(command), expects)
        if(no_finalize == False):
            self.finalize()
