#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@文件        :tsio.py
@时间        :2021/09/18 11:00:25
@作者        :weiqs
@邮箱        :weiqs@asiainfo.com
@版本        :1.0
@说明        :不同场景数据读取方法，数据加载与转化 -> pd.DataFrame
'''

import pandas as pd

def load_file_1(filename: str) -> pd.DataFrame:
    data = pd.read_csv(filename)
    data = data[["_source.gathTime", "_source.metricValue"]]
    return data 

def load_file_2(filename: str) -> pd.DataFrame:
    with open(filename, "r") as fp:
        data = [eval(line.strip()) for line in fp.readlines()]
    data = pd.DataFrame(data)
    data = data[["seqTime", "trainIndexValue"]]
    return data

def load_file_3(filename: str) -> pd.DataFrame:
    data = pd.DataFrame(filename)
    return data