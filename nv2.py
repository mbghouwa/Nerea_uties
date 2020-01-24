import os
import logging as log
from sys import argv
from datetime import datetime
from time import time


def timed_message(message):
    return "{}: {}".format(str(datetime.now())[:19], message)


if __name__ == '__main__':
    beg = time()
    worker_number = int(argv[1])
    log.basicConfig(filename=f"logs/worker_{worker_number:02d}_log", level=log.DEBUG)
    with open(f"worker_files/f_{worker_number:02d}.txt", "r") as fwl:
        files = fwl.read().split("\n")[:-1]
    compt = 1
    for f in files:
        checkpoint = time()
        os.system(f'python dict_filler.py "{f}"')
        print(timed_message(f"Worker {worker_number:02d} File {compt:04d}/{len(files)}: {f} ({time() - checkpoint:.2f}s)."))
        log.info(timed_message(f"File {compt:04d}/{len(files)}: {f} took --> {time() - checkpoint:.2f}s."))
        compt += 1
