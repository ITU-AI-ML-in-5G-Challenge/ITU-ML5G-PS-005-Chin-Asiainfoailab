#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@文件        :regression.py
@时间        :2021/09/18 11:13:43
@作者        :weiqs
@邮箱        :weiqs@asiainfo.com
@版本        :1.0
@说明        : 回归方法
'''

import pandas as pd
import numpy  as np
from sklearn import linear_model
from statsmodels.tsa.seasonal import seasonal_decompose
from common.preprocess import process

def _mae(real, pred, risk):
    t = 0
    for x, y in zip(np.asarray(real), np.asarray(pred)):
        if x > y:
            t += risk * (x - y)
        else:
            t += (1 - risk) * (y -x)
    if len(real) > 0:
        return t / len(real)
    else:
        return 0

def _train(time_series: pd.Series, period: int) -> dict :
    reg = linear_model.LinearRegression()
    x = pd.DataFrame()
    for i in range(period + 1):
        x[i] = time_series.shift(i)
    x = x.dropna()
    train_size = int(0.8 * time_series.shape[0])
    train_x, test_x = x.iloc[: train_size] , x.iloc[train_size: ] 
    reg.fit(train_x.iloc[:,1: ], train_x[0])
    pred_y = reg.predict(test_x.iloc[:, 1:])
    real_y = test_x[0]
    test_diff = (real_y - pred_y).abs()
    # mae_value = _mae(real_y, pred_y, risk=0.5)
    model = {
        "coef": reg.coef_,
        "intercept" : reg.intercept_ ,
        "e_std": test_diff.std()
    }
    return model

def detect(time_series: pd.Series, model: dict):

    sample_rate = model["attr"]["sample_rate"]
    period = model["attr"]["period"]
    reg_model = model["reg"]
    # kde_model = model["kde"]
    # stat_model = model["stat"]
    seasonal_mapping = model["seasonal"]
    time_series = process(time_series, sample_rate)
    # 滑动平均处理
    trend = time_series.rolling(period).mean()
    # 周期项映射
    seasonal = [seasonal_mapping["%02d%02d" %
                                 (k.hour, k.minute)] for k in time_series.index]
    seasonal = pd.Series(seasonal, index=time_series.index)
    # 计算残差项
    resid = time_series - trend - seasonal
    # 趋势项预测
    pred_trend = trend.dropna().rolling(period).apply(
        lambda x: x.dot(reg_model["coef"]) + reg_model["intercept"], raw=True)
    # resid_result = [kde.detect(v, kde_model) for v in resid]
    # resid_result = pd.DataFrame(
    #     resid_result, index=resid.index, columns=["bottom", "top"])
    # result = resid_result + pred_trend + seasonal
    return None

def train(time_series: pd.Series, cycle="1D") -> dict:
    period = pd.Timedelta(
        cycle) // pd.Timedelta(time_series.index.inferred_freq)
    # 人工输入周期长度，进行周期性校验
    if time_series.shape[0] < 2 * period:
        raise Exception("时序长度%d, 小于2个周期(%d)的长度。" %
                        (time_series.shape[0], 2 * period))
    result_add = seasonal_decompose(time_series, model='additive', filt=np.repeat(
        1./period, period), freq=period, two_sided=False)
    seasonal = result_add.seasonal.dropna()
    mapping = {}
    for k, v in seasonal.head(period).iteritems():
        mapping["%02d%02d" % (k.hour, k.minute)] = v
    resid = result_add.resid.dropna()
    trend = result_add.trend.dropna()
    # 训练趋势预测线性模型
    reg_model = _train(trend, period)
    return {
        "reg": reg_model,
        "seasonal": mapping,
        "resid": resid.std(),
        "attr": {
            "sample_rate": time_series.index.freq,
            "period": period
        }
    }