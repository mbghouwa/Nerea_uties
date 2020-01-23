import numpy as np
import pandas as pd
from sys import argv
from scipy.stats import linregress
import os
import json


def clean_name(s):
    suppo = s.split("/")[-1][:-4]
    suppo = suppo.split("_")
    return suppo


def contains_information_climate(s):
    for element in s:
        if "CLIMATE" in element:
            return True
        else:
            pass
    return False


def contains_information_cleaned(s):
    for element in s:
        if "CLIMATE CLEANED" in element:
            return True
        else:
            pass
    return False


def contains_information_not(s):
    for element in s:
        if "NOT" in element:
            return True
        else:
            pass
    return False


def create_ref(date_beg, date_end):
    return np.arange(date_beg, date_end, dtype="datetime64[D]")


def create_risk_period_range(beg_signature, end_signature, year_beg, year_end):
    r = list()
    for y in range(year_beg, year_end):
        d_beg, d_end = np.datetime64("{}-{}".format(y, beg_signature)), np.datetime64("{}-{}".format(y, end_signature))
        r.append(create_ref(d_beg, d_end))
    return np.concatenate(r)


def get_ratio(dataframe, variable_name, reference=None, year_end=2019, year_beg=1980):
    if reference is not None:
        arr1 = reference
    else:
        arr1 = create_ref(np.datetime64("{}-01-01".format(year_beg)), np.datetime64("{}-01-01".format(year_end+1)))
    arr2 = dataframe[variable_name]
    diff_dates = np.setdiff1d(arr1, arr2)
    return diff_dates, diff_dates.size / arr1.size


def payout(x, trigger, exitp=8, SI=1000000):
    if trigger < exitp:
        return np.nan
    else:
        tick = SI / (trigger - exitp)
        return min(SI, max(trigger - x, 0) * tick)


bs, es = "06-01", "08-01"
qs = [5, 10, 15, 20]
strikes = [20, 30, 40, 50, 60]
probs = pd.read_csv("probability.csv")

if __name__ == '__main__':
    filename = argv[1]
    try:
        df = pd.read_csv(filename)
        df["DAILY_RAIN"] = df["DAILY_RAIN"].replace("NIL", np.nan).astype(float)
        df["DATE"] = pd.to_datetime(df["DATE"])
        summary = df.iloc[0, :].to_dict()
        summary["DATE"] = str(summary["DATE"])
        summary["date_begin"], summary["date_end"] = df["DATE"].agg(["min", "max"])
        if summary["date_begin"] >=  np.datetime64(("{}-"+bs).format(summary["date_begin"].dt.year)):
            summary["date_begin"] =  np.datetime64("{}-01-01".format(summary["date_begin"].dt.year +1))
            df = df[df["DATE"] >= summary["date_begin"]]
        summary["date_begin"] = str(summary["date_begin"])
        summary["date_end"] = str(summary["date_end"])
        df_efficace = df[df["DAILY_RAIN"].notna()]
        summary["eff_begin"], summary["eff_end"] = df_efficace["DATE"].agg(["min", "max"]).astype(str)
        summary["TYPE"] = (np.round(df.groupby("TYPE")["DAILY_RAIN"].count()/df["DAILY_RAIN"].count(), 2)*100).to_dict()
        summary["all_types"] = df["TYPE"].unique().tolist()
        summary["nb_duplicates"] = df["DATE"].size - df.drop_duplicates()["DATE"].size
        summary["duplicate_percentage"] = 1 - (df.drop_duplicates()["DATE"].size / df["DATE"].size)
        summary["ratios"] = dict()
        summary["missing_dates"] = dict()
        for year, name in zip([1980, 1999, 2009], ["1980", "10_years", "20_years"]):
            transitory = df_efficace[df_efficace["DATE"] >= np.datetime64("{}-01-01".format(year))]
            diff, ratio = get_ratio(transitory, "DATE", year_beg=year, year_end=2019)
            summary["ratios"]["ratio_{}".format(name)] = ratio
            summary["missing_dates"]["missing_{}".format(year)] = list(diff.astype(str))
        df_efficace = df_efficace[df_efficace["DATE"] >= np.datetime64("1980-01-01")]
        df_rp = df_efficace.copy()
        df_rp = df_rp[df_rp["DATE"].dt.month.isin([6, 7])]
        summary["rp_ratios"] = dict()
        summary["rp_missing_dates"] = dict()
        for year, name in zip([1980, 1999, 2009], ["1980", "10_years", "20_years"]):
            transitory = df_rp[df_efficace["DATE"] >= np.datetime64("{}-01-01".format(year))]
            ref = create_risk_period_range(beg_signature=bs, end_signature=es,
                                           year_beg=year, year_end=2019)
            diff, ratio = get_ratio(transitory, "DATE", reference=ref)
            summary["ratios"]["ratio_rp_{}".format(name)] = ratio
            summary["missing_dates"]["missing_rp_{}".format(year)] = list(diff.astype(str))
        df_rp["year"] = df_rp["DATE"].dt.year
        data_index = df_rp.groupby("year")["DAILY_RAIN"].agg([("DAILY_RAIN", np.sum)]).reset_index()
        data_index['rank'] = data_index["DAILY_RAIN"].rank()
        rank_dictionary = dict(zip(["rank_{:02d}".format(q) for q in data_index["year"]], data_index['rank']))
        summary.update(rank_dictionary)
        percentiles = np.percentile(data_index["DAILY_RAIN"], q=qs)
        percentiles_dictionary = dict(zip(["percentile_untrended_{:02d}".format(q) for q in qs], percentiles))
        summary.update(percentiles_dictionary)
        prob_terciles = prob.loc[prob["Name"] == filename.loc[0,"Name"], ["tp_probb","tp_probn","tp_proba"]].to_numpy()[0]
        prob_terciles = prob_terciles/pd.qcut(data_index["DAILY_RAIN"], 3).value_counts().tolist()
        data_index['weight'] = pd.qcut(data_index['DAILY_RAIN'], 3, labels = prob_terciles).astype(float)
        for strike in strikes:
            data_index['payout_' + str(strike)] = data_index['DAILY_RAIN'].apply(lambda x: payout(x, strike))
        BC_all = np.round((data_index['weight'].reshape(-1,1)*data_index.iloc[:, -4:]).sum(axis=0), decimals=0)
        BC_20 = np.round((data_index['weight'].reshape(-1,1)[-20:]*data_index.iloc[-20:, -4:]).sum(axis=0), decimals=0)
        BC_10 = np.round((data_index['weight'].reshape(-1,1)[-10:]*data_index.iloc[-10:, -4:]).sum(axis=0), decimals=0)
        BC_5 = np.round((data_index['weight'].reshape(-1,1)[-5:]*data_index.iloc[-5:, -4:]).sum(axis=0), decimals=0)
        BC_all_dict = dict(zip(["BC_all_" + d for d in data_index.columns[-4:]], BC_all))
        BC_20_dict = dict(zip(["BC_20_" + d for d in data_index.columns[-4:]], BC_20))
        BC_10_dict = dict(zip(["BC_10_" + d for d in data_index.columns[-4:]], BC_10))
        BC_5_dict = dict(zip(["BC_5_" + d for d in data_index.columns[-4:]], BC_5))
        summary.update(**BC_all_dict, **BC_20_dict, **BC_10_dict, **BC_5_dict)
        slope = linregress(data_index.index, data_index['DAILY_RAIN'])[0]
        data_index['detrend'] = data_index['DAILY_RAIN'] - slope * data_index['year'] + slope * 2019
        data_index.loc[data_index['detrend'] < 0, 'detrend'] = 0
        summary.update({"slope": slope})
        for strike in strikes:
            data_index['payout_detrend_' + str(strike)] = data_index['detrend'].apply(lambda x: payout(x, strike))
        BC_d_all = np.round((data_index['weight'].reshape(-1,1)*data_index.iloc[:, -4:]).sum(axis=0), decimals=0)
        BC_d_20 = np.round((data_index['weight'].reshape(-1,1)[-20:]*data_index.iloc[-20:, -4:]).sum(axis=0), decimals=0)
        BC_d_10 = np.round((data_index['weight'].reshape(-1,1)[-10:]*data_index.iloc[-10:, -4:]).sum(axis=0), decimals=0)
        BC_d_5 = np.round((data_index['weight'].reshape(-1,1)[-5:]*data_index.iloc[-5:, -4:]).sum(axis=0), decimals=0)
        BC_d_all_dict = dict(zip(["BC_detrend_all_" + d for d in data_index.columns[-4:]], BC_d_all))
        BC_d_20_dict = dict(zip(["BC_detrend_20_" + d for d in data_index.columns[-4:]], BC_d_20))
        BC_d_10_dict = dict(zip(["BC_detrend_10_" + d for d in data_index.columns[-4:]], BC_d_10))
        BC_d_5_dict = dict(zip(["BC_detrend_5_" + d for d in data_index.columns[-4:]], BC_d_5))
        summary.update(**BC_d_all_dict, **BC_d_20_dict, **BC_d_10_dict, **BC_d_5_dict)
        data_index.rename(columns={"DAILY_RAIN": "untrended_index"}, inplace=True)
        c_name = summary["CITY"].replace(" ", "_").replace("(", "").replace(")", "").replace("'", "").replace("&", "")
        os.system("mkdir -p data/{}".format(c_name))
        data_index.to_csv("data/{}/{}_payouts.csv".format(c_name, c_name), index=False)
        with open("data/{}/{}_summary.json".format(c_name, c_name), "w") as fp:
            fp.write(json.dumps(summary, indent=4))
    except Exception as ex:
        ex_message = f"An error of type {type(ex).__name__} occurred. Arguments:\n{ex.args}"
        c_name = filename.split("CLIMATE CLEANED")[0]
        c_name = c_name.replace(" ", "_").replace("(", "").replace(")", "").replace("'", "").replace("&", "")
        os.system("mkdir -p data/{}".format(c_name))
        summary = {"CITY": c_name}
        with open("data/{}/{}_summary.json".format(c_name, c_name), "w") as fp:
            fp.write(json.dumps(summary, indent=4))
