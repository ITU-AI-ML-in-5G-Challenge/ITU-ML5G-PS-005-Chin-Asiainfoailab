#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@文件        :kde.py
@时间        :2021/09/18 12:18:14
@作者        :weiqs
@邮箱        :weiqs@asiainfo.com
@版本        :1.0
@说明        : KDE 异常检测方法
'''

import pandas as pd
from common.preprocess import process, unit_index
import numpy  as np

def train(time_series: pd.Series, bins: int = 20, ar=None, limit="both"):
    
    q0 = time_series.min()
    q1 = time_series.max()
    if ar is not None and ar > 0:
        if limit == "both":
            q0 = time_series.quantile(ar / 2) 
            q1 = time_series.quantile(1 - ar/2)
        elif limit == "up":
            q1 = time_series.quantile(1 - ar)
        elif limit == "down":
            q0 = time_series.quantile(ar)
    if q0 < 0:
        q0 = 0
    q0 = round(q0, 4)
    q1 = round(q1, 4)

    if len(time_series.unique()) <= bins:
        return {
            "q0": q0,
            "q1": q1
        }
    else:
        base = (q1 - q0) / bins
        labeled = pd.DataFrame()
        labeled["value"] = time_series
        labeled["label"] = (time_series - q0) // base
        labeled["unit"] = unit_index(time_series)
        labeled = labeled[(labeled["value"] >= q0) & (labeled["value"] <= q1)]

        bins_count = labeled["label"].value_counts()
        model = {
            "q0": q0,
            "q1": q1,
            "base" : base,
            "length" : time_series.shape[0]
        }
        model["bins"] = bins_count.to_dict()

        unit_map = {}
        for u, part in labeled.groupby("unit"):
            arr = np.asarray(part["label"])
            if len(arr) == 0 :
                continue
            m = np.nanmean(arr)
            s = np.nanstd(arr)
            top = round(m + 3 * s)
            bottom = int(m - 3 * s)
            if bottom < 0:
                bottom = 0
            if top > bins:
                top = bins
            unit_map[u] = [bottom, top]
        model["unit"] = unit_map
        return model

def detect(time_series: pd.Series, model: dict) -> dict:
    def check(x):
        q0 = model["q0"]
        q1 = model["q1"]
        base = model.get("base", None)
        if base is None:
            if x["value"] < q0:
                bottom, top, fit = q0, q0, q0
                anomaly = -1
            elif x["value"] > q1:
                bottom, top, fit = q1, q1, q1
                anomaly = 1
            else:
                bottom, top, fit = x["value"], x["value"], x["value"]
                anomaly = 0
        else:
            unit_map = model["unit"]
            bin_limit = unit_map.get(x["unit"], [0, 0])
            bin_bottom, bin_top = bin_limit[0], bin_limit[1]
            bin_id = (x["value"] - q0) // base

            if bin_id < bin_bottom:
                bottom = bin_bottom * base + q0
                top = bottom + base
                fit = bottom + 0.5 * base
                anomaly = -1
            elif bin_id > bin_top:
                top = bin_top * base + q0
                bottom = top - base
                fit = top - 0.5 * base
                anomaly = 1
                if bottom < 0:
                    bottom = 0
                if fit < 0:
                    fit = 0
            else:
                bottom = int(bin_id) * base + q0
                if bin_bottom == bin_top:
                    top = bottom
                    fit = bottom
                else:
                    top = bottom + base
                    fit = bottom + 0.5 * base
                anomaly = 0
        
        return pd.Series([top, bottom, anomaly], index=["top", "bottom", "anomaly"])

    result = pd.DataFrame()
    result["value"] = time_series
    result["unit"] = unit_index(time_series)
    result[["top", "bottom", "anomaly"]] = result.apply(check, axis=1)
    return result

if __name__ == '__main__':

    filename = "E:/data/aiops/input_data/K00716.csv"
    df = pd.read_csv(filename, header=None)
    t = process(df)
    step = 180
    train_part = t.iloc[: step]
    test_part = t.iloc[step: ]
    model = train(train_part, bins=20, ar=0.04)
    a = detect(test_part, model)
    print(a.head())