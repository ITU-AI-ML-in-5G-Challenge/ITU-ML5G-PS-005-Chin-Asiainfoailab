#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@文件        :test_tsfresh.py
@时间        :2021/11/18 19:23:03
@作者        :weiqs
@邮箱        :weiqs@asiainfo.com
@版本        :1.0
@说明        :
'''

import sys
sys.path.append("D:/pythonProject/aiops_tsad_boosting")

import pandas as pd
import tsfresh
import datetime

def change_type(t: str):
    a = datetime.datetime.strptime(t, "%Y/%m/%d")
    return datetime.datetime.strftime(a, "%Y-%m-%d 00:00:00")

if __name__ == '__main__':
    # filename = "E:/data/aiops/input_data/K07816.csv"
    filename = "E:/data/aiops/input_data/51af28a310d145cd8ecf2ddca0489e20&indexId=avg_time"
    tmp = pd.read_csv(filename)
    tmp = tmp[["_source.gathTime", "_source.metricValue"]]
    tmp = tmp.sort_values("_source.gathTime")
    
    data = []
    for _, row in tmp.iterrows():
        data.append({"time": row[0], "value": row[1]})
    step = 2880
    train_params = {
        "data": data[: step],
        "parameters": {}
    }

    res = tsfresh.train_api(train_params)

    df = pd.DataFrame(data[step: ])
    df.to_csv("E:/data/aiops/input_data/temp.csv", index=False, header=None)

    import json
    with open("E:/data/aiops/model.json", "w") as fp:
        fp.write(json.dumps(res["model"]))