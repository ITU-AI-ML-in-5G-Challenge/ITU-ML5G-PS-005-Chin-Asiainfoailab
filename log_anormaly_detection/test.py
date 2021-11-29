#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@文件        :test.py
@时间        :2021/10/26 18:13:01
@作者        :weiqs
@邮箱        :weiqs@asiainfo.com
@版本        :1.0
@说明        :
'''

import json
import re
import pandas as pd


if __name__ == '__main__':
    reg = r'x[0-9a-f]{2,8}$'
    s = "x5BFk"
    a = re.fullmatch(reg, s.lower())
    print(a)

    # a = pd.DataFrame()
    # a.iterrows()

    # data = []
    # cnt = 0
    # with open("E:/data/aiops/error_message.txt", "r", encoding="utf-8") as fp:
    #     for line in fp:
    #         s = line.strip()
    #         # print(s)
    #         d = json.loads(s, strict=False)
    #         # print(d["body"])
    #         # print(d["timestamp"])
    #         print(d)
    #         data.append(d)
    #         break
    #         # s = re.sub(r'[\r\n]+', "", s)
    # print(data)