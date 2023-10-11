from __future__ import print_function
from colorama import Fore, Back, Style

match_calls = 0

def reset_match_calls():
    global match_calls
    match_calls = 0

def get_match_calls():
    global match_calls
    return match_calls

def match(step, outcome, expected):
    global match_calls
    match_calls += 1
    print(Style.RESET_ALL)
    if("{}".format(outcome) == expected):
        print (Style.RESET_ALL+"STEP:   {}{}\nOK  :   {}{}".format(Fore.BLUE, step, Fore.GREEN, outcome))
        return 1
    else:
        print (Style.RESET_ALL+"\nSTEP:   {}{}\nFAIL:   {}{}\nEXPECT: {}{}\n".format(Fore.BLUE, step, Fore.RED, outcome, Fore.CYAN, expected))
        return 0
