#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@文件        :spark_log_parse.py
@时间        :2021/10/24 20:03:28
@作者        :weiqs
@邮箱        :weiqs@asiainfo.com
@版本        :1.0
@说明        :
'''

from pyspark import SparkContext, SparkConf
import re
from pyspark.rdd import RDD
from operator import add
from tqdm import tqdm
import json
from simhash import Simhash
import requests
from ftp_tool import FTPTool
import pandas as pd
import traceback

import os, sys
current_dir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(current_dir)


def parse_func(s):
    f = re.findall("[.a-zA-Z0-9\u4e00-\u9fa5]+", s)
    return f

def split_phrases(body: str) -> list:
    arr = parse_func(body)
    arr.append("*")
    return list(set(arr))

def process_words_count(lines: RDD) :
    counts = lines.map(split_phrases) \
                  .flatMap(lambda x: x) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    return output

def process_hash_code(arr: list):
    if len(arr) == 1 and arr[0] == "*":
        return "F"
    code = Simhash(arr, f=16).value
    hex_code = (str(hex(code))[2: ]).upper()
    fill_code = ("%4s" % hex_code).replace(" ", "0")
    return "E" + fill_code , " ".join(arr)

def process_template_words(words: list, ratio=0.002) -> dict:
    # 提取模板词
    #只能匹配1、12、123等只包含数字的字符串
    reg_int= r'^\d+$' 
    #能匹配2.36、0.36、00069.63、0.0、263.25等
    reg_float= r'^\d+\.\d+$'
    # 匹配16进制的数值类型
    reg_hex = r'^[0-9a-f]{2,8}$'
    # 匹配 IP
    reg_ip = r'(([01]{0,1}\d{0,1}\d|2[0-4]\d|25[0-5])\.){3}([01]{0,1}\d{0,1}\d|2[0-4]\d|25[0-5])'
    sort_items = sorted(words, key=lambda x: x[1], reverse=True)
    print(sort_items[0])
    thres = sort_items[0][1] * ratio + 1
    template_words = {}
    rule_words = set()
    for v in sort_items:
        k = v[0]
        if re.fullmatch(reg_int, k) is not None:
            continue
        elif re.fullmatch(reg_float, k) is not None:
            rule_words.add(k)
            continue
        elif re.fullmatch(reg_ip, k) is not None:
            rule_words.add(k)
            continue
        elif re.fullmatch(reg_hex, k) is not None:
            rule_words.add(k)
            continue
        elif v[1] < thres:
            continue
        else:
            template_words[k] = v[1]

    return {
        "template": template_words,
        "rule" : rule_words
    }


def format_line(arr, words):
    res = []
    rules = set()
    for k in arr:
        if k in words["template"]:
            res.append(k)
        else:
            if k in words["rule"]:
                rules.add(k)
            if len(res) == 0:
                res.append("*")
            elif res[-1] == "*":
                continue
            else:
                res.append("*")
    return res, rules


def process_template_sequence(lines: RDD, template_words: dict):
    counts = lines.map(lambda x: (parse_func(x), x)) \
                  .map(lambda x: (format_line(x[0], template_words) , x[1])) \
                  .map(lambda x: (x[0][0], x[0][1] , x[1])) \
                  .map(lambda x: (process_hash_code(x[0]) , x[1], x[2])) \
                  .map(lambda x: (x[0][0], x[0][1], x[1], x[2])) \
                  .groupBy(lambda x: x[0]) \
                  .map(lambda x: (x[0], list(x[1])[0], len(list(x[1]))))
    # return counts.collect()
    res = []
    for k in counts.collect():
        res.append({
            "logTemplateId": k[0],
            "logTemplate": k[1][1],
            "TemplateSampleWithParameter" : "".join(["<span>%s</span>" % j for j in k[1][2]]),
            "TemplateSample": k[1][3],
            "updateFlag" : 1,
            "TemplateCount": k[2]
        })
    return res

def post_message(url, ai_task_id, code, msg):
    data = {
        "aiTaskId": str(ai_task_id),
        "code": code,
        "msg": msg
    }
    headers = {'Content-Type': 'application/json', "Accept": "*/*"}
    response = requests.post(url, json=data, headers=headers)
    if response.status_code == 200:
        print(response.json())
    else:
        print(response)

def download_files(ftp_config, remote_path, local_path):
    host = ftp_config["host"]
    username = ftp_config["username"]
    password = ftp_config["password"]
    port = int(ftp_config["port"])

    ftp_tool = FTPTool(host, port, username, password)
    ftp_tool.downloaddirs(remote_path, local_path)
    ftp_tool.release()
    return 1

def upload_files(ftp_config, remote_path, local_path):
    host = ftp_config["host"]
    username = ftp_config["username"]
    password = ftp_config["password"]
    port = int(ftp_config["port"])

    ftp_tool = FTPTool(host, port, username, password)
    ftp_tool.uploaddirs(remote_path, local_path)
    ftp_tool.release()
    return 1

def create_spark_context():
    spark_conf = SparkConf().setAppName('log-parser')
    sc = SparkContext(conf=spark_conf)
    print('<# spark version = %s > ' % str(sc.version))
    return sc

def test():
    ftpUrl = "ftp://ftpUser:ftP!123@10.15.49.41:22021/data/input/test/train_data.csv"
    sc = create_spark_context()
    file = sc.wholeTextFiles(ftpUrl)
    res = file.collect()
    print(res[: 2])
    sc.stop()

if __name__ == '__main__':

    ftpUrl = sys.argv[1]
    template_path = sys.argv[2]
    pickle_path = sys.argv[3]
    url = sys.argv[4]
    params = json.loads(sys.argv[5])
    task_id = params["ai_task_id"]
    print(task_id)
    ftp_config = json.loads(sys.argv[6])

    try: 
        sc = create_spark_context()
        file = sc.wholeTextFiles(ftpUrl, minPartitions=12)
        rdd = file.map(lambda x: x[1]) \
                  .map(lambda x: x.split("\n")[1: ]) \
                  .flatMap(lambda x: x) \
                  .map(lambda x: x.strip().split(',')) \
                  .filter(lambda x: len(x) > 1) \
                  .map(lambda x: x[1: ]) \
                  .map(lambda x: ",".join(x))

        rdd.isEmpty()
        # rdd.persist(storagelevel.StorageLevel.MEMORY_AND_DISK)
        words = process_words_count(rdd)
        print("##########################################")
        print(len(words))
        # print(words)
        template_words = process_template_words(words)
        print("########################################")
        print(len(template_words))
        # print(template_words)
        broadcast_words = sc.broadcast(template_words)
        template_sequences = process_template_sequence(rdd, broadcast_words.value)
        # print(template_sequences)
        print(len(template_sequences))

        data = pd.DataFrame(template_sequences)
        data = data[(data["TemplateCount"] >= 20) & (data["logTemplate"] != "*")]

        template_filename = os.path.join(current_dir, "template.csv")
        data.to_csv(template_filename, index=False)
        upload_files(ftp_config, template_path, template_filename)

        words_filename = os.path.join(current_dir, "template_words.json")
        with open(words_filename, "w", encoding="utf-8") as g:
            g.write(json.dumps(template_words["template"], ensure_ascii=False))

        upload_files(ftp_config, pickle_path, words_filename)
        post_message(url, task_id, 0, "success")
        sc.stop()
        print('success!')

    except Exception as e:
        print(e)
        post_message(url, task_id, 1, str(e))
        traceback.print_exc()