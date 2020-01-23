import os
from time import time as timer
from datetime import datetime
import pandas as pd
import json


def timed_message(message):
    return "{}: {}".format(str(datetime.now())[:19], message)


specials_variables = ["TYPE", "ratios", "missing_dates"]


if __name__ == '__main__':
    beg = timer()
    skip = False
    with open("track.txt", "r") as t:
        skip = "2100/2100" in t.read()
    print(skip)
    if not skip:
        with open("track.txt", "a") as tr:
            os.system("aws s3 cp s3://storage-underwriter/stations/ ./stations/ --recursive")
            tr.write(timed_message("Complete download took : {}s".format(timer() - beg)))
            l_files = os.popen("ls stations/*.csv").read().split("\n")[:-1]
            compt = 1
            for f in l_files:
                checkpoint = timer()
                os.system('python dict_filler.py "{}"'.format(f))
                tr.write(timed_message("File {:04d}/2100 took {:.2f}s.\n".format(compt, timer() - checkpoint)))
                compt += 1
    else:
        pass
    with open("track.txt", "a") as tr:
        dir_names = os.popen("ls data").read().split("\n")[:-1]
        f_names = ["data/{}/{}_summary.json".format(d, d) for d in dir_names]
        l_dict = list()
        compt = 1
        for f in f_names:
            with open(f, "r") as jd:
                print(f)
                checkpoint = timer()
                tr.write(timed_message("File {:04d}/2100 took {:.2f}s.\n".format(compt, timer() - checkpoint)))
                tempo = json.load(jd)
                l_dict.append(tempo)
            compt += 1
        general = pd.DataFrame(l_dict)
        specials = [pd.DataFrame(list(general[spec])) for spec in specials_variables]
        general_modified = general.drop(specials_variables, axis=1)
        gl = pd.concat([general_modified] + specials[:-1], axis=1)
        g = pd.concat([general_modified] + specials, axis=1)
        gl.to_csv("data/general_wo_missing.csv", index=False)
        g.to_csv("data/general_w_missing.csv", index=False)
