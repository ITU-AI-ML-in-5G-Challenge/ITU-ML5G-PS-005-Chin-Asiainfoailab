#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@文件        :boosting.py
@时间        :2021/09/24 14:08:26
@作者        :weiqs
@邮箱        :weiqs@asiainfo.com
@版本        :1.0
@说明        :
'''

import pandas as pd
import sys
from common.preprocess import process
from algorithm import cluster, statistic, regression, kde

sys.path.append("D:/pythonProject/aiops_tsad_boosting")

eps = 1e-6

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
    check_window = parameters.get("check_window", 120)
    train_model = parameters.get("train_model", True)

    parameters = params.get("parameters", {})
    sample_rate = parameters.get("sample_rate", None)
    cycle = parameters.get("cycle", "1D")
    anomaly_ratio = parameters.get("anomaly_ratio", 0.04)
    data = pd.DataFrame(params['data'])
    ts = process(data, sample_rate)
    # ret = cluster.train(ts, ar=anomaly_ratio)
    ret = statistic.train(ts, cycle)
    return ret


def detect_api(params: dict) -> dict:
    data = pd.DataFrame(params["data"])
    model = params["model"]
    ts = process(data)
    return None
    # return detect(data, model)


if __name__ == '__main__':

    from common.tsio import load_file_1
    tmp_data = load_file_1(
        "E:/data/aiops/input_data/85eec72df0b7443d9aaa7e6c5faadf39&indexId=oracle_active_connects/e8be1730-6d54-43ef-8334-f4a58f1948fe")
    tmp_train_part = tmp_data.iloc[: int(len(tmp_data) * 0.8)]
    tmp_test_part = tmp_data.iloc[int(len(tmp_data) * 0.8):]