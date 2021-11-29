#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@文件        :local_log_match.py
@时间        :2021/10/24 20:03:20
@作者        :weiqs
@邮箱        :weiqs@asiainfo.com
@版本        :1.0
@说明        :
'''

import re
from tqdm import tqdm
import json
from simhash import Simhash

def parse_func(s):
    f = re.findall("[.a-zA-Z0-9\u4e00-\u9fa5]+", s)
    return f

def format_line(arr, words):
    res = []
    for k in arr:
        if k in words:
            res.append(k)
        else:
            if len(res) == 0:
                res.append("*")
            elif res[-1] == "*":
                continue
            else:
                res.append("*")
    return res

def execute_matches(line, seq_index, word_index):
    try:
        d = json.loads(line.strip())
        body = d["body"].strip()
        arr = parse_func(body)
        formart_arr = format_line(arr, word_index)
        hash_value = Simhash(formart_arr).value
        t = seq_index.get(str(hash_value), {}).get("cnt", 0)
        if t < 2:
            return False
        else:
            return str(hash_value)
    except:
        return None
    
if __name__ == '__main__':

    with open("./data/index_counts.json", "r") as fp:
        index_count = json.load(fp)
    with open("./data/template_words.json", 'r') as fp:
        words = json.load(fp)

    error_logs = {}