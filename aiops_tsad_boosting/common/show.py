#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@文件        :show.py
@时间        :2021/09/18 11:14:22
@作者        :weiqs
@邮箱        :weiqs@asiainfo.com
@版本        :1.0
@说明        :图形展示
'''

import  matplotlib.pyplot as plt
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

def show(data):
    plt.figure(figsize=(16,4))
    plt.plot(data) # 将真实值绘制为散点图（默认为深蓝色）
    plt.show()

def show_anomaly(data):
    plt.figure(figsize=(16,4))
    plt.fill_between(data.index, data['bottom'], data['top'], color='#7F7FFF50') # 将模型预测区间绘制为浅蓝紫色阴影
    plt.plot(data["value"]) # 将真实值绘制为散点图（默认为深蓝色）
    abnormal_points = data[data['anomaly'] != 0]["value"]
    plt.scatter(abnormal_points.index, abnormal_points,  s=5, c='r') # 将真实值中的异常值绘制为稍大的红点
    plt.show()