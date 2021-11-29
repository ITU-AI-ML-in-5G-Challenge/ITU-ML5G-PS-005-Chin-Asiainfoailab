#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@文件        :preprocess.py
@时间        :2021/09/18 10:58:52
@作者        :weiqs
@邮箱        :weiqs@asiainfo.com
@版本        :1.0
@说明        : 时序数据预处理方法
'''

import pandas as pd
import numpy as np
from statsmodels.tsa.seasonal import seasonal_decompose


def process(data, sample_rate=None, na_threshold=0.5) -> pd.Series:
    # 重定义列名
    data.columns = ["ts_time", "ts_value"]
    data = data.dropna()
    # 时间字符串转时间格式
    data["ts_time"] = pd.to_datetime(data["ts_time"])
    # 时间排序
    data = data.sort_values("ts_time")
    # 自动识别采样率
    if sample_rate is None:
        time_diff = data["ts_time"].diff() // pd.Timedelta("1S")
        sr = int(time_diff.value_counts().index[0]) * pd.Timedelta("1S")
    else:
        sr = pd.Timedelta(sample_rate)
    # 统一时间格式
    baseline = pd.to_datetime("2000-01-01 00:00:00")
    data["ts_time"] = (data["ts_time"] - baseline) // sr * sr + baseline
    # 按时间去重，提取最大值
    duplicate_df = data.groupby("ts_time").max()
    # 提取空值比例
    total = (duplicate_df.index.max() - duplicate_df.index.min()) // sr
    if total == 0:
        raise Exception("最大时间间隔小于采样粒度！")
    length = duplicate_df.shape[0]
    na_ratio = 1 - length / total
    if na_ratio > na_threshold:
        raise Exception("最大长度%d, 实际长度%d, 空值比例%.2f" %
                        (total, length, na_ratio))
    supplement_df = duplicate_df.asfreq(freq=sr, method="ffill")
    result_ts = supplement_df["ts_value"]
    return result_ts.dropna()

def filter_values(time_series: pd.Series, ar=0.04, limit="both") -> pd.Series:
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
    time_series[time_series < q0] = q0
    time_series[time_series > q1] = q1
    return time_series


def unit_index(time_series):
    if pd.Timedelta(time_series.index.freq) / pd.Timedelta("1D") < 1:
        index_list = ["%02d%02d" % (x.hour, x.minute)
                      for x in time_series.index]
    else:
        index_list = ["%02d" % int(x.day % x.days_in_month)
                      for x in time_series.index]
    return index_list


def modified_sigmoid(x):
    return 1 / (1 + np.exp(-x))


def ts_feature(time_series, usage_para=0.8, k=3, trend_threshold=0.9, step_threshold=0.9, cycle="1D", seasonal_threshold=0.9):
    eps = 1e-6
    feature = {
        'graph_type': {
            'graph': -1,
            'Quantify': -1
        },
        'trend_type': {
            'trend': -1,
            'Quantify': -1
        },
        'rush_type': {
            'rush': -1,
            'Quantify': -1
        }
    }
    feature['usage'] = 1 if time_series.min() > usage_para else 0
    diff = time_series.diff().dropna()
    _min, _max = diff.min(), diff.max()
    q1, q2, q3 = np.percentile(diff, [25, 50, 75])
    iqr = q3 - q1
    max_limit = q3 + (k * iqr)
    min_limit = q1 - (k * iqr)
    if _min < min_limit and _max > max_limit:
        pass
    elif _min < min_limit:
        feature['rush_type']['rush'] = 2
        feature['rush_type']['Quantify'] = modified_sigmoid(
            (q2 - _min) / (q2 - min_limit + eps) - 1)
    elif _max > max_limit:
        feature['rush_type']['rush'] = 1
        feature['rush_type']['Quantify'] = modified_sigmoid(
            (_max - q2) / (max_limit - q2 + eps) - 1)
    else:
        feature['rush_type']['rush'] = 0
        upper_quantify = (max_limit - _max) / (max_limit - q2 + eps) / 2 + 0.5
        lower_quantify = (_min - min_limit) / (q2 - min_limit + eps) / 2 + 0.5
        quantify = np.nanmax([upper_quantify, lower_quantify])
        if quantify > 1:
            quantify = 1
        feature['rush_type']['Quantify'] = quantify

    narray = np.asarray(time_series)
    median = abs(np.nanmedian(narray))
    # 趋势判断
    trend_diff = abs(narray[-1] - narray[0])
    if trend_diff > median * trend_threshold:
        if trend_diff < 0:
            feature['trend_type']['trend'] = 2
            feature['trend_type']['Quantify'] = 1.0
        else:
            feature['trend_type']['trend'] = 1
            feature['trend_type']['Quantify'] = 1.0
    else:
        feature['trend_type']['trend'] = 0
        feature['trend_type']['Quantify'] = 0

    # 阶梯型判断
    zero_diff_ratio = np.mean(diff == 0)
    if zero_diff_ratio > step_threshold:
        feature['graph_type']['graph'] = 2
        feature['graph_type']['Quantify'] = (
            zero_diff_ratio - step_threshold) / (1 - step_threshold) / 2 + 0.5
        feature['seasonal_num'] = -1

    # 周期型判断
    period = pd.Timedelta(cycle) // pd.Timedelta(time_series.index.freq)
    if len(time_series) < 2 * period + 1:
        feature['graph_type']['graph'] = 0
        feature['graph_type']['Quantify'] = 0
        feature['seasonal_num'] = -1
    else:
        result_add = seasonal_decompose(
            time_series, 'additive', None, period, two_sided=False)
        seasonal = result_add.seasonal.dropna()
        resid = result_add.resid.dropna()
        seasonal_std = seasonal.std()
        resid_std = resid.std()
        seasonal_std = seasonal_std + eps
        resid_std = resid_std + eps
        seasonal_resid_ratio = seasonal_std / resid_std
        seasonal_bond = seasonal.max() - seasonal.min()
        data_bond = time_series.mean()
        seasonal_bond_ratio = seasonal_bond / data_bond
        if seasonal_resid_ratio > seasonal_threshold and seasonal_bond_ratio >= 0.08:
            feature['graph_type']['graph'] = 1
            feature['graph_type']['Quantify'] = modified_sigmoid(
                seasonal_resid_ratio / seasonal_threshold - 1)
            feature['seasonal_num'] = period
        else:
            feature['graph_type']['graph'] = 0
            feature['graph_type']['Quantify'] = modified_sigmoid(
                seasonal_resid_ratio / seasonal_threshold - 1)
            feature['seasonal_num'] = -1
            
    return feature


if __name__ == '__main__':
    filename = "E:/data/aiops/input_data/51af28a310d145cd8ecf2ddca0489e20&indexId=avg_time"
    tmp = pd.read_csv(filename)
    tmp = tmp[["_source.gathTime", "_source.metricValue"]]
    t = process(tmp)
    print(t.head())
    print(type(t))
    # print(t)
