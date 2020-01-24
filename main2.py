import os
from time import time as timer
from datetime import datetime
import pandas as pd
import json


def timed_message(message):
    return "{}: {}".format(str(datetime.now())[:19], message)


specials_variables = ["TYPE", "ratios", "missing_dates"]

dir_names = os.popen("ls data").read().split("\n")[:-1]
f_names = ["data/{}/{}_summary.json".format(d, d) for d in dir_names]
l_dict = list()
compt = 1
with open("tracks_v2.txt","w") as tr:
    for f in f_names:
        with open(f, "r") as jd:
            print(f)
            checkpoint = timer()
            tr.write(timed_message("File {:04d}/{} took {:.2f}s.\n".format(compt, len(f_names), timer() - checkpoint)))
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