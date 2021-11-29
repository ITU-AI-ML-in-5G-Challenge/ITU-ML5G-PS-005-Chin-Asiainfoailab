#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@文件        :cluster.py
@时间        :2021/10/17 13:53:49
@作者        :weiqs
@邮箱        :weiqs@asiainfo.com
@版本        :1.0
@说明        :
'''

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from common.preprocess import unit_index
from common.show import show_anomaly

def train(time_series: pd.Series, k=20):
    c = time_series.unique()
    q0 = time_series.min()
    q1 = time_series.max()
    if len(c) <= k:
        centers = np.sort(c)
        model = {
            "q0": q0,
            "q1": q1,
            "centers": list(centers)
        }
        return model
    kmeans = KMeans(n_clusters=k)
    x = pd.DataFrame(data=time_series)
    kmeans.fit(x)
    centers = kmeans.cluster_centers_
    centers = np.sort(centers.reshape(-1))
    centers = np.round(centers, 4)
    labeled = pd.DataFrame()
    for i, k in enumerate(centers):
        labeled[i] = round(np.abs(time_series - k), 4)
    labeled["label"] = labeled.idxmin(axis=1)
    labeled["unit"] = unit_index(time_series)
    labeled["center"] = labeled["label"].apply(lambda x: centers[x])
    
    unit_map = {}
    cv_cnt_0 = 0
    cv_cnt_1 = 0
    for u, part in labeled.groupby("unit"):
        arr = np.asarray(part["center"])
        if len(arr) == 0 :
            unit_map[u] = [0, 0]
        m = np.nanmean(arr)
        s = np.nanstd(arr)
        cv = 0 if m == 0 else s / m
        if cv > 0.7:
            cv_cnt_1 += 1
        elif cv == 0:
            cv_cnt_0 += 1
        top = round(m + 3 * s)
        bottom = int(m - 3 * s)
        if bottom < 0:
            bottom = 0
        unit_map[u] = [bottom, top]
        
    dis = [max(t[i]) for i, t in labeled.groupby("label")]
    if cv_cnt_1 / len(unit_map) < 0.3 and cv_cnt_0 / len(unit_map) < 0.5:
        return {
            "q0": q0,
            "q1": q1,
            "unit": unit_map,
            "dis": dis,
            "centers": list(centers)
        }
    else:
        return {
            "q0": q0,
            "q1": q1,
            "dis": dis,
            "centers": list(centers)
       }
    
def detect(time_series: pd.Series, model: dict) -> dict:
    centers = model["centers"]
    q0 = model["q0"]
    q1 = model["q1"]
    
    def check(x):
        if x["value"] < q0:
            bottom, top, fit = q0, q0, q0
            anomaly = -1
        elif x["value"] > q1:
            bottom, top, fit = q1, q1, q1
            anomaly = 1
        else:
            center = centers[x["label"]]
            cur_dis = x["value"] - center
            dis = model.get("dis", None)
            unit_map = model.get("unit", None)
            
            if dis is None or unit_map is None:
                if cur_dis < 0:
                    top = center
                    if x["label"] > 0:
                        bottom = centers[x["label"] - 1]
                    else:
                        bottom = centers[x["label"]]
                else:
                    bottom = center
                    if x["label"] < len(centers) - 1:
                        top = centers[x["label"] + 1]
                    else:
                        top = centers[x["label"]]
                fit = round((top + bottom) / 2, 4)
                anomaly = 0
            else:            
                center_limit = unit_map.get(x["unit"], [0, 0])
                center_bottom = center_limit[0] - dis[x["label"]]
                center_top = center_limit[1] + dis[x["label"]]

                if x["value"] < center_bottom :
                    bottom = center_bottom
                    top = bottom + dis[x["label"]] + cur_dis
                    fit = bottom + cur_dis
                    anomaly = -1
                elif x["value"] > center_top:
                    top = center_top
                    bottom = top - dis[x["label"]] - cur_dis
                    fit = top - cur_dis
                    anomaly = 1
                else:
                    bottom = center - cur_dis - dis[x["label"]]
                    top = center + cur_dis + dis[x["label"]]
                    fit = center
                    anomaly = 0
                
            if bottom < q0 :
                bottom = q0
            elif bottom > q1:
                bottom = q1
            if top > q1:
                top = q1
            elif top < q0:
                top = q0
                
        return pd.Series([top, bottom, anomaly], index=["top", "bottom", "anomaly"])
            
    labeled = pd.DataFrame()
    for i, c in enumerate(centers):
        labeled[i] = np.abs(time_series - c)
    
    result = pd.DataFrame()
    result["value"] = time_series
    result["unit"] = unit_index(time_series)
    result["label"] = labeled.idxmin(axis=1)
    result[["top", "bottom", "anomaly"]] = result.apply(check, axis=1)
    return result


if __name__ == '__main__':
    filename = "E:/data/aiops/input_data/K07751.csv"
    tmp = pd.read_csv(filename)
    from common.preprocess import process, filter_values
    t = process(tmp)
    step = 180
    train_part = t.iloc[: step]
    test_part = t.iloc[step: ]
    filter_train_part = filter_values(train_part, ar=0.02)
    model = train(filter_train_part)
    a = detect(test_part, model)
    from common.show import show_anomaly
    # show(t)
    show_anomaly(a)