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
import json
from simhash import Simhash
import requests
from ftp_tool import FTPTool
import pandas as pd
import traceback

import os, sys
current_dir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(current_dir)

REG = r"[._a-zA-Z0-9\u4e00-\u9fa5]+"

def parse_func(s):
    f = re.findall(REG, s)
    return f

def check_rule(s: str) -> bool:
    #只能匹配1、12、123等只包含数字的字符串
    reg_int= r'^\d+$' 
    #能匹配2.36、0.36、00069.63、0.0、263.25等
    reg_float= r'^\d+\.\d+$'
    # 匹配16进制的数值类型
    reg_hex = r'^[0-9a-f]{2,8}$'
    # 匹配x开头的16进制数值类型
    reg_hex_x = r'x[0-9a-f]{2,8}$'
    # 匹配 IP
    reg_ip = r'(([01]{0,1}\d{0,1}\d|2[0-4]\d|25[0-5])\.){3}([01]{0,1}\d{0,1}\d|2[0-4]\d|25[0-5])'
    if re.fullmatch(reg_int, s) is not None:
        return True
    elif re.fullmatch(reg_float, s) is not None:
        return True
    elif re.fullmatch(reg_ip, s) is not None:
        return True
    elif re.fullmatch(reg_hex, s.lower()) is not None:
        return True
    elif re.fullmatch(reg_hex_x, s.lower()) is not None:
        return True
    elif len(s) < 4:
        return True
    elif s.lower() =="null":
        return True
    else:
        return False

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

def process_template_words(words: list, ratio=0.001) -> dict:
    sort_items = sorted(words, key=lambda x: x[1], reverse=True)
    print(sort_items[0])
    thres = sort_items[0][1] * ratio + 1
    template_words = {}
    for v in sort_items:
        k = v[0]
        if check_rule(k):
            continue
        elif v[1] < thres:
            continue
        else:
            template_words[k] = v[1]
    return template_words

def format_line(arr, words):
    res = []
    for k in arr:
        v = k if k in words else "*"
        if len(res) == 0:
            res.append(v)
        elif res[-1] != "*":
            res.append(v)
        elif v != "*":
            res.append(v)
    return res

def process_hash_code(arr: list):
    if len(arr) == 1 and arr[0] == "*":
        return "F"
    code = Simhash(arr, f=16).value
    hex_code = (str(hex(code))[2: ]).upper()
    fill_code = ("%4s" % hex_code).replace(" ", "0")
    return "E" + fill_code

def process_template_sequence(lines: RDD, template_words: dict):
    counts = lines.map(parse_func) \
                  .map(lambda x: format_line(x, template_words)) \
                  .map(lambda x: (process_hash_code(x), " ".join(x))) \
                  .groupBy(lambda x: x[0], numPartitions=12) \
                  .map(lambda x: (x[0], list(x[1])[0], len(list(x[1]))))
    res = []
    for k in counts.collect():
        res.append({
            "logTemplateId": k[0],
            "logTemplate": k[1][1],
            "TemplateSample": "",
            "TemplateSampleWithParameter" : "",
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
    spark_context = SparkContext(conf=spark_conf)
    print('<# spark version = %s > ' % str(spark_context.version))
    return spark_context

def test():
    ftpUrl = "ftp://ftpUser:ftP!123@10.15.49.41:22021/data/input/test/train_data.csv"
    sc = create_spark_context()
    file = sc.wholeTextFiles(ftpUrl)
    res = file.collect()
    print(res[: 2])
    sc.stop()

def execute(spark: SparkContext, rdd: RDD):
    rdd = rdd.map(lambda x: x[1: ]) \
                 .flatMap(lambda x: x) \
                 .map(lambda x: x.strip().split(',')) \
                 .filter(lambda x: len(x) > 1) \
                 .map(lambda x: x[1: ]) \
                 .map(lambda x: ",".join(x))

    words = process_words_count(rdd)
    print("##########################################")
    print(len(words))
    template_words = process_template_words(words)
    print("########################################")
    print(len(template_words))
    # print(template_words)
    broadcast_words = spark.broadcast(template_words)
    template_sequences = process_template_sequence(rdd, broadcast_words.value)
    # print(template_sequences)
    print(len(template_sequences))

    data = pd.DataFrame(template_sequences)
    data = data[(data["TemplateCount"] >= 20) & (data["logTemplateId"] != "F")]
    cols = [
        'logTemplateId', 
        'logTemplate', 
        'TemplateSample',
        'TemplateSampleWithParameter',
        'updateFlag',
        'TemplateCount'
        ]
    data = data[cols]
    # 保存
    template_filename = os.path.join(current_dir, "template.csv")
    data.to_csv(template_filename, index=False)

    index_count = {}
    for _, row in data.iterrows():
        index_count[row["logTemplateId"]] = row["TemplateCount"]
    index_filename = os.path.join(current_dir, "index_counts.json")
    with open(index_filename, "w", encoding="utf-8") as g:
        g.write(json.dumps(index_count, ensure_ascii=False))

    words_filename = os.path.join(current_dir, "template_words.json")
    with open(words_filename, "w", encoding="utf-8") as g:
        g.write(json.dumps(template_words, ensure_ascii=False))

    return 1


if __name__ == '__main__':

    ftpUrl = sys.argv[1]
    template_path = sys.argv[2]
    pickle_path = sys.argv[3]
    url = sys.argv[4]
    params = json.loads(sys.argv[5])
    task_id = params["ai_task_id"]
    ftp_config = json.loads(sys.argv[6])

    try: 
        sc = create_spark_context()
        file = sc.wholeTextFiles(ftpUrl, minPartitions=12)

        data_rdd = file.map(lambda x: x[1]) \
                  .map(lambda x: x.split("\n")) \
                  .filter(lambda x: len(x) > 10) \
        
        if data_rdd.isEmpty():
            post_message(url, task_id, 1, "file is empty!")
            sc.stop()
            print("file is empty")
        else:
            execute(sc, data_rdd)
            upload_files(ftp_config, template_path, os.path.join(current_dir, "template.csv"))
            upload_files(ftp_config, pickle_path, os.path.join(current_dir, "index_counts.json"))
            upload_files(ftp_config, pickle_path, os.path.join(current_dir, "template_words.json"))
            post_message(url, task_id, 0, "success")
            sc.stop()
            print('success!')

    except Exception as e:
        print(e)
        post_message(url, task_id, 1, str(e))
        traceback.print_exc()