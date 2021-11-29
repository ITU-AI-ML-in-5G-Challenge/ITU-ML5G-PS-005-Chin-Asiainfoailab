#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@文件        :tsfresh.py
@时间        :2021/11/18 17:39:52
@作者        :weiqs
@邮箱        :weiqs@asiainfo.com
@版本        :1.0
@说明        :
'''

import pandas as pd
from common.preprocess import process, ts_feature, filter_values
from algorithm import cluster

def train_api(params: dict) -> dict:
    parameters = params.get("parameters", {})
    step_threshold = parameters.get('step_threshold', 0.9)
    trend_threshold = parameters.get('trend_threshold', 0.9)
    seasonal_threshold = parameters.get('seasonal_threshold', 1.0)
    IQR_coef = parameters.get('IQR_coef', 2.0)
    usage_para = parameters.get('usage_para', 0.8)
    sample_rate = parameters.get("sample_rate", None)
    cycle = parameters.get("cycle", "30D")
    anomaly_ratio = parameters.get("anomaly_ratio", 0.02)
    anomaly_limit = parameters.get("anomaly_limit", "both")
    check_window = parameters.get("check_window", 120)
    cluster_k = parameters.get("cluster_k", 20)
    train_model = parameters.get("train_model", True)
    input_data = pd.DataFrame(params['data'])
    process_ts = process(input_data, sample_rate)
    features = ts_feature(process_ts, usage_para, IQR_coef, trend_threshold, step_threshold, cycle, seasonal_threshold)
    if train_model :
        train_ts = filter_values(process_ts, anomaly_ratio, anomaly_limit)
        model = cluster.train(train_ts, cluster_k)
        model["sample_rate"] = pd.Timedelta(train_ts.index.freq) // pd.Timedelta("1S")
        features["model"] = model

    return features

def detect_api(params: dict) -> dict:
    input_data = pd.DataFrame(params['data'])
    model = params["model"]
    sample_rate = "%dS" % model["sample_rate"]
    process_ts = process(input_data, sample_rate)
    a = cluster.detect(process_ts, model)
    return a.to_dict()