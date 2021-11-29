#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@文件        :local_log_parse.py
@时间        :2021/10/24 19:42:00
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

def process_words_count(lines: list) :
    word_count = {}
    for line in tqdm(lines, desc="process words ... "):
        for w in set(line):
            word_count[w] = word_count.get(w, 0) + 1
    return word_count

def process_template_words(words: dict) -> dict:
    # 提取模板词
    #只能匹配1、12、123等只包含数字的字符串
    reg_int= r'^\d+$' 
    #能匹配2.36、0.36、00069.63、0.0、263.25等
    reg_float= r'^\d+\.\d+$'
    # 匹配16进制的数值类型
    reg_hex = r'^[0-9a-f]{2,8}$'
    # 匹配 IP
    reg_ip = r'(([01]{0,1}\d{0,1}\d|2[0-4]\d|25[0-5])\.){3}([01]{0,1}\d{0,1}\d|2[0-4]\d|25[0-5])'
    template_words = {}
    for k, v in words.items():
        if v < 1000:
            continue
        elif re.fullmatch(reg_int, k) is not None:
            continue
        elif re.fullmatch(reg_float, k) is not None:
            continue
        elif re.fullmatch(reg_ip, k) is not None:
            continue
        elif re.fullmatch(reg_hex, k) is not None:
            continue
        else:
            template_words[k] = v

    return template_words

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


def process_template_sequence(lines: list, words: dict):
    index_count = {}
    for line in tqdm(lines, desc="process sequence ... "):
        formart_arr = format_line(line, words)
        hash_value = Simhash(formart_arr, f=16).value
        info = index_count.get(hash_value, {})
        info["cnt"] = info.get("cnt", 0) + 1
        info["seq"] = " ".join(formart_arr)
        index_count[hash_value] = info
    return index_count

if __name__ == '__main__':

    data = []
    with open("./data/test.log", "r") as fp:
        for line in tqdm(fp, desc="load data ... "):
            try:
                d = json.loads(line.strip())
                data.append(d)
            except:
                continue

    # 文本分词
    split_data = []
    for line in tqdm(data, desc="split lines ... "):
        arr = parse_func(line["body"])
        split_data.append(arr)
    # 词频统计
    words = process_words_count(split_data)
    # 获取模板词
    template_words = process_template_words(words)
    # 提取模板
    template_sequences = process_template_sequence(split_data, template_words)
    print(len(template_sequences))
    print('success!')