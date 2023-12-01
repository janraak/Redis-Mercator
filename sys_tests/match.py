from __future__ import print_function
from colorama import Fore, Back, Style
import pdb
import hashlib
import ast
import traceback
import sys

match_calls = 0

def find_local_ip():
    for l in os.popen("ip -o -4 a").read().split("\n"):
        xface = l.split(' ')
        if xface[1] != 'lo':
            for c in range(2, len(xface)):
                if(xface[c] == 'inet'):
                    return xface[c+1].split('/')[0]
    return '127.0.0.1'


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

def list_to_hash(h,l):
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
                h.update(bytes(l, 'utf-8'))
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
            list_to_hash(h,e)
        else:
            h.update(bytes(e))
    return h

def flatten_list(o,l):
    if l is None:
        return o
    if isinstance(l, dict):
        for key in l:
            o.append(key)
            flatten_list(o,l[key])
        return o
    if not isinstance(l, list):
        o.append(l)
        return o
    for e in l:
        if isinstance(e, dict):
            flatten_list(o,e)
        elif isinstance(e, list):
            flatten_list(o,e)
        else:
            o.append(e)
    return o
        

def match_hashed(step, outcome, expected):
    if isinstance(outcome, dict):
        outcome = flatten_list([],outcome)
    if isinstance(expected, dict):
        expected = flatten_list([],expected)
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
        list_to_hash(eHasher,expected)
        if rHasher.digest() == eHasher.digest():
            print(
                Style.RESET_ALL
                + "STEP:   {}{}".format(
                    Fore.GREEN, step
                )
            )
            return 1
        fo = flatten_list([],outcome)
        fe = flatten_list([],expected)
        if match_sets(step, fo, fe) == 1:
            return 1
        return 0
        print(
            Style.RESET_ALL
            + "STEP:   {}{}\nFAIL(H):   {}{}\nEXPECT: {}{}\n".format(
                Fore.BLUE, step, Fore.RED, outcome, Fore.CYAN, expected
            )
        )
    except Exception as ex:
        # traceback.print_exception(ex, Exception, "Match failed")
        print(" failed, error: {}".format(ex))
        traceback.print_exc() 
        # pdb.set_trace()
    return 0


def match_sets(step, outcome, expected):
    if isinstance(expected, str):
        expected = ast.literal_eval(expected)
    if isinstance(outcome, dict):
        outcome = flatten_list([],outcome)
        pdb.set_trace()
    if expected is dict:
        expected = flatten_list([],expected)
        pdb.set_trace()

    global match_calls
    match_calls += 1
    print(Style.RESET_ALL)
    try:
        rSet = set(flatten_list([],outcome))
        eSet = set(flatten_list([],expected))
        diff = rSet.symmetric_difference(eSet)
        if len(diff) == 0:
            print(
                Style.RESET_ALL
                + "STEP:   {}{}".format(
                    Fore.GREEN, step
                )
            )
            return 1
        return match_strings(step,outcome,expected)
        print(
            Style.RESET_ALL
            + "STEP:   {}{}\nFAIL(S):   {}{}\nEXPECT: {}{}\nDiff: {}{}\n".format(
                Fore.BLUE, step, Fore.RED, outcome, Fore.CYAN, expected, Fore.YELLOW, diff
            )
        )
        return 0
    except TypeError as ex:
        pdb.set_trace()
    except Exception as ex:
        # traceback.print_exception(ex, Exception, "Match failed")
        print(" failed, error: {}".format(ex))
        # print("outcome {} {}",type(outcome),outcome)
        # print("expected {} {}",type(expected),expected)
        traceback.print_exc() 
        # pdb.set_trace()
        return 0


def match_strings(step, outcome, expected):
    global match_calls
    match_calls += 1
    print(Style.RESET_ALL)
    if not isinstance(outcome, str):
        outcome = "{}".format(outcome)
    if not isinstance(expected, str):
        expected = "{}".format(expected)
    if expected[0] == "*":
        if (expected[1:]) in outcome:
            print(
                Style.RESET_ALL
                + "STEP:   {}{}".format(
                    Fore.GREEN, step
                )
            )
            return 1
        else:
            print(
                Style.RESET_ALL
                + "STEP:   {}{}\nFAIL(T):   {}{}\nEXPECT: {}{}\n".format(
                    Fore.BLUE, step, Fore.RED, outcome, Fore.CYAN, expected
                )
            )
            return 0
    elif (
        len(expected) > 128
        and left(outcome, 64) == left(expected, 64)
        and right(outcome, 64) == right(expected, 64)
    ):
        print(
            Style.RESET_ALL
            + "STEP:   {}{}".format(Fore.GREEN, step)
        )
        return 1
    elif "{}".format(outcome) == expected:
        print(
            Style.RESET_ALL
            + "STEP:   {}{}".format(Fore.GREEN, step)
        )
        return 1
    else:
        print(
            Style.RESET_ALL
            + "STEP:   {}{}\nFAIL:   {}{}\nEXPECT: {}{}\n".format(
                Fore.BLUE, step, Fore.RED, outcome, Fore.CYAN, expected
            )
        )
        return 0


def match(step, outcome, expected):
    return match_hashed(step,outcome,expected)

def stripField(outcome, field):
    if not isinstance(outcome,list):
        return outcome
    result =[]
    for r in outcome:
        pi = r.index(field)
        if not pi is None:
            r = r[0:pi]+ r[pi+2:]
        result.append(r)

    return result

def filterRows(outcome, field, values):
    if not isinstance(outcome,list):
        return outcome
    result =[]
    for r in outcome:
        pi = r.index(field)
        if not pi is None:
            # pdb.set_trace()
            if r[pi + 1] in values:
                result.append(r)

    return result

def HeaderOnly(outcome):
    return stripField(outcome,b'path')


class Matcher:
    def __init__(self, test_name) -> None:
        self.succes_tally = 0
        self.match_calls = 0
        self.test_name = test_name


    def by_hashed(self, step, outcome, expected):
        self.match_calls += 1
        matched = match_hashed(step,outcome, expected)
        self.succes_tally += matched
        return matched


    def by_sets(self, step, outcome, expected):
        self.match_calls += 1
        matched = match_sets(step,outcome, expected)
        self.succes_tally += matched
        return matched


    def by_strings(self, step, outcome, expected):
        self.match_calls += 1
        matched = match_strings(step,outcome, expected)
        self.succes_tally += matched
        return matched

    def finalize(self):            
        if self.succes_tally != self.match_calls: 
            raise AssertionError( "FAILED: {}, {} of {} steps failed".format(self.test_name, self.match_calls - self.succes_tally, self.match_calls))
