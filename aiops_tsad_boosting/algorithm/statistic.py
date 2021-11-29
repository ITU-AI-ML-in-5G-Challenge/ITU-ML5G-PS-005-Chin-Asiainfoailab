#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@文件        :statistic.py
@时间        :2021/09/18 11:37:51
@作者        :weiqs
@邮箱        :weiqs@asiainfo.com
@版本        :1.0
@说明        : 分片的 3-sigma 准则
'''

import pandas as pd
import numpy as np

def sigma(arr: np.array , n: float = 3.0, ratio: float = 0.04) -> list:
    q0 = np.quantile(arr, ratio)
    q1 = np.quantile(arr, 1 - ratio)
    normal = arr[(q0 <= arr) & (arr <= q1)]
    m = np.nanmean(normal)
    s = np.nanstd(normal)
    return [float(m + n * s), float(m - n * s), float(m)]

def train(time_series: pd.Series, cycle="1D") -> dict:
    seasonal_ts = time_series.copy()
    sr = time_series.index.inferred_freq
    unit = "HM"
    index_list = ["%02d%02d" % (x.hour, x.minute) for x in seasonal_ts.index]
    if cycle[-1] == "M" or sr[-1] == "D":
        unit = "D"
        index_list = ["%02d" % (x.day) for x in seasonal_ts.index]

    seasonal_ts.index = index_list
    seasonal_model = {}
    for i, s in  seasonal_ts.groupby(level=0):
        seasonal_model[i] = sigma(np.asarray(s))

    model = {
        "unit": unit,
        "model": seasonal_model
    }
    return model

def detect(time_series: pd.Series, model: dict) -> pd.DataFrame:
    def check(x):
        if x["value"] > x["top"]:
            return 1
        elif x["value"] < x["bottom"]:
            return 1
        else:
            return 0

    anomaly_result = pd.DataFrame()
    anomaly_result["value"] = time_series
    index_list = ["%02d%02d" % (x.hour, x.minute) for x in time_series.index]
    if model["unit"] == "D":
        index_list = ["%02d" % x.day for x in time_series.index]
    check_result = [model.get(x, [0, 0, 0]) for x in index_list]
    check_df = pd.DataFrame(check_result, columns=["top", "bottom", "fit_value"], index=time_series.index)
    anomaly_result = pd.concat([anomaly_result, check_df], axis=1)
    anomaly_result["anomaly"] = anomaly_result.apply(check, axis=1)
    return anomaly_result
    