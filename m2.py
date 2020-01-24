import os
import logging as log
import numpy as np
from datetime import datetime

def timed_message(message):
    return "{}: {}".format(str(datetime.now())[:19], message)

num_workers = 10
l_files = os.popen("ls stations/*.csv").read().split("\n")[:-1]

log.basicConfig(filename=f"logs/general_log", level=log.DEBUG)

ind = np.arange(0,len(l_files)+1, len(l_files) // num_workers)[:-1]
s_e = np.concatenate((ind[:-1].reshape(-1,1), ind[1:].reshape(-1,1)), axis=1)

log.info(timed_message("Parallel JSON Generation Init"))

i = 0
while i < num_workers - 1:
    log.info(timed_message(f"Creating worker {i + 1}."))
    supp = l_files[s_e[i,0]:s_e[i,1]]
    log.info(timed_message(f"Worker {i + 1}: Handling {len(supp):03d} files."))
    log.info(timed_message(f"First Index {s_e[i, 0]} --> Last Index {s_e[i, 1]}"))
    with open(f"worker_files/f_{i+1:02d}.txt","w") as fs:
        fs.write("\n".join(supp))
        fs.write("\n")
    i += 1

log.info(timed_message(f"Creating worker {i + 1}."))
supp = l_files[s_e[-1,-1]:]
log.info(timed_message(f"Worker {i + 1}: Handling {len(supp):03d} files."))
log.info(timed_message(f"First Index {s_e[-1, -1]} --> Last Index {None}"))
with open(f"worker_files/f_{i+1:02d}.txt","w") as fs:
    fs.write("\n".join(supp))
    fs.write("\n")

for w in range(1, num_workers + 1):
    log.info(timed_message(f"Just launched worker number {w:02d}"))
    os.system(f"python nv2.py {w:02d} &")

