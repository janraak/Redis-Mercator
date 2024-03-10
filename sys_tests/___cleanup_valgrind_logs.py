import glob
import os
import pdb

def read_entry(file):
    entry = []
    while True:
        try:
            line = file.readline()
        except:
            break
        if not line:
            file.close()
            break
        if len(line) < 16:
            return entry
        if "--" == line[0:2]:
            continue
        if len(entry) == 0:
            if not "Thread " in line \
                and not "== Conditional" in line \
                and not "== Use of uninitialised" in line \
                and not "loss record" in line:
                continue
        entry.append(line)
        
    return entry
def is_ignored(loc, ignores):
    for i in ignores:
        if len(i) == 0 or "[?!]" in i:
            continue
        if loc in i:
            return True
    return False

def cleanup_log(log, ignores):
    print (log)
    root = log[0:log.rfind('/') + 1]
    out = log.replace(".log", "-cleansed.log")
    clean = open(out, 'w')
    file = open(log,'r')
    file.closed
    while not file.closed and file.readable():
        try:
            entry = read_entry(file)
            if entry and len(entry) > 0:
                skip_entry = False
                mercator_location = None
                for line in entry:
                    # if "in loss record" in line:
                    #     pdb.set_trace()
                    if "serverCron" in line:
                        skip_entry = True
                        break
                    if "rdb" in line:
                        skip_entry = True
                        break
                    if "spt" in line:
                        skip_entry = True
                        break
                    if "lua" in line:
                        skip_entry = True
                        break
                    if "_dl_" in line:
                        skip_entry = True
                        break
                    # if "Sjiboleth" in line:
                    #     skip_entry = False
                    #     break
                    if mercator_location is None:
                        if "rx" in line \
                            or "sji" in line \
                            or "client-" in line:
                                left_colon = line.find(":")
                                right_colon = line.find(" ", left_colon + 2)
                                left_bracket = line.rfind('(')
                                right_bracket = line.rfind(':', left_bracket)
                                fn = line[left_colon+2: left_bracket - 1]
                                if left_bracket > 0 and right_bracket > left_bracket:
                                    mercator_location = line[left_bracket+1:right_bracket + 1] + fn
                if not skip_entry:
                    clean.writelines(entry)
                    clean.write("\n")
                    if mercator_location and not is_ignored(mercator_location, ignores):
                        locFn = (root + "src_" + mercator_location + ".log").replace(':', "_")
                        loc = open(locFn, 'a')
                        loc.writelines(entry)
                        loc.write("\n")
                        loc.close()
                        
        except:
            pass
    clean.close()
    

ign = open("./valgrind_ignore.lst", "r")
ignores = ign.readlines()
ign.close()

for log in glob.glob("./run_logs/src_*.log"):
    os.remove(log)

for log in glob.glob("./run_logs/_valgrind-*.log"):
    if not "-redis-" in log and  not "-cleansed." in log :
        cleanup_log(log, ignores)
