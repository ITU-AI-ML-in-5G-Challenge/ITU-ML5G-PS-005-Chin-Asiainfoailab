#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@文件        :spark_log_match.py
@时间        :2021/10/24 22:25:06
@作者        :weiqs
@邮箱        :weiqs@asiainfo.com
@版本        :1.0
@说明        :
'''

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
import sys
import json
import os
import re
from simhash import Simhash
from ftp_tool import FTPTool
from tqdm import tqdm

import os, sys
current_dir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(current_dir)

ftp_path = sys.argv[1]
ftp_config = json.loads(sys.argv[2])
brokers = sys.argv[3]
topic = sys.argv[4]
topic_out = sys.argv[5]

REG = r"[._a-zA-Z0-9\u4e00-\u9fa5]+"

def load_multiple_models(path):
    models = {}
    if os.path.exists(path):
        for model_folder in os.listdir(path):
            try:
                models[model_folder] = load_single_models(os.path.join(path, model_folder))
                print("加载模型文件成功")
            except Exception as e:
                print('模型文件读取失败', e)
    return models

def load_single_models(path):
    words_file = os.path.join(path, "template_words.json")
    index_file = os.path.join(path, "index_counts.json")
    with open(words_file, 'r') as fp:
        words = json.load(fp)
    with open(index_file, 'r') as fp:
        seqs = json.load(fp)
    return {
        "word_index": words,
        "seq_index" : seqs
    }

# Send kafka message
def send_msg(msg, producer):
    msgs = msg.collect()
    # msgs_count = len(msgs)
    # logger_obj.info("Write result {0} records to kafka".format(msgs_count))
    for rec in msgs:
        #logger_obj.info("send result {} to kafka".format(rec))
        producer.send(topic_out, rec)
    if len(msgs) > 0:
        producer.flush()

def download_files(ftp_config, remote_path, local_path):
    host = ftp_config["host"]
    username = ftp_config["username"]
    password = ftp_config["password"]
    port = int(ftp_config["port"])

    ftp_tool = FTPTool(host, port, username, password)
    ftp_tool.downloaddirs(remote_path, local_path)
    ftp_tool.release()
    return 1

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

def reformat_line(arr: list, pos: int=10) -> list:
    res = []
    for k in arr:
        v = "*" if check_rule(k) else k
        if len(res) == 0:
            res.append(v)
        elif len(res) >= pos:
            break
        elif res[-1] != "*":
            res.append(v)
        elif v != "*":
            res.append(v)
    return res

def extract_line(body: str, words: dict) -> str:
    reg_iter = re.finditer(REG, body)
    sub_list = []
    pos = 0
    for k in reg_iter:
        if k.group() not in words:
            start_pos, end_pos = k.span()
            sub_list.append(body[pos: start_pos])
            sub_list.append("<span>%s</span>" % k.group())
            pos = end_pos
    sub_list.append(body[pos: ])
    return "".join(sub_list)

def process_hash_code(arr: list):
    if len(arr) == 1 and arr[0] == "*":
        return "F"
    code = Simhash(arr, f=16).value
    hex_code = (str(hex(code))[2: ]).upper()
    fill_code = ("%4s" % hex_code).replace(" ", "0")
    return "E" + fill_code

def log_process(models, log_content):
    # attribute
    attribute = log_content.get("attributes", {})
    # 组织
    organization = attribute.get("organization", "")
    # 日志类型
    log_type = attribute.get("logType", "")
    # 日志对象
    res_name = log_content.get("resource", {}).get("name")
    # concatID
    model_id = "&".join([organization, log_type, res_name])
    model = models.get(model_id, {})
    word_index = model.get("word_index", None)
    seq_index = model.get("seq_index", None)

    log_body = log_content["body"].strip()
    log_time = log_content["timestamp"]
    parse_arr = parse_func(log_body)
    res = {
        "logObject": res_name,
        "logType": log_type,
        "organization": organization,
        "logTime": log_time,
        "logMessage": "",
        "logMessageRegularization": "",
        "logTemplateId": "",
        "logDetail": "",
        "logAnomalyId" : ""
    }

    if word_index is None or seq_index is None:
        res["logMessage"] = extract_line(log_body, {})
        res["logDetail"] = "attributes or resource is None"
        res["logTemplateId"] = "F"
        format_arr = reformat_line(parse_arr)
        res["logAnomalyId"] = process_hash_code(format_arr)
        res["logMessageRegularization"] = " ".join(format_arr)
        return res
    else:
        res["logMessage"] = extract_line(log_body, word_index)
        format_arr = format_line(parse_arr, word_index)
        if len(format_arr) == 0 and format_arr[0] == "*":
            format_arr = reformat_line(parse_arr)
        code = process_hash_code(format_arr)
        res["logMessageRegularization"] = " ".join(format_arr)
        if code in seq_index:
            res["logTemplateId"] = code
            res["logDetail"] = "match"
        else:
            res["logDetail"] = "not match"
            res["logTemplateId"] = "F"
            res["logAnomalyId"] = code
        return res

def load_json(raw):
    try:
        r = json.loads(raw, strict=False)
        r["attributes"]["organization"]
        r["attributes"]["logType"]
        r["resource"]["name"]
        # concatID
        r["body"]
        r["timestamp"]
        return r
    except :
        return None

def create_spark_context():
    spark_conf = SparkConf().set("spark.python.profile", "true").set(
        "spark.io.compression.codec", "snappy")
    spark_context = SparkContext(conf=spark_conf)
    print('<# spark version = %s > ' % str(spark_context.version))
    return spark_context

def kafka_consumer():
    pkl_files = os.path.join(current_dir, "pkl_files")
    download_files(ftp_config, ftp_path, pkl_files)
    model = load_multiple_models(pkl_files)
    batch_interval = 100
    sc = create_spark_context()
    ssc = StreamingContext(sc, batch_interval)
    kvs = KafkaUtils.createDirectStream(
        ssc, [topic], kafkaParams={"metadata.broker.list": brokers})
    # broadcase models to worker nodes
    brodcast_model = sc.broadcast(model)
    # Producer to write result backto kafka
    producer = KafkaProducer(bootstrap_servers=brokers,
                             value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    # Get content
    lines = kvs.map(lambda x: x[1]) \
               .map(load_json) \
               .filter(lambda x: x is not None)

    # message match template
    log_predict = lines.map(lambda x: log_process(brodcast_model.value, x))
    # result to kafka
    log_predict.foreachRDD(lambda x: send_msg(x, producer))
    ssc.start()
    ssc.awaitTermination()

def local_consumer():
    pkl_files = os.path.join(current_dir, "pkl_files")
    download_files(ftp_config, ftp_path, pkl_files)
    model = load_multiple_models(pkl_files)
    # batch_interval = 100
    filename = "./test.log"
    data = []
    with open(filename, "r") as fp:
        for line in tqdm(fp, desc="load data ... "):
            d = load_json(line.strip())
            if d is not None:
                data.append(d)

    sc = create_spark_context()
    brodcast_model = sc.broadcast(model)
    rdd = sc.parallelize(data, numSlices=12)
    lines = rdd.map(lambda x: log_process(brodcast_model.value, x))
    result = lines.collect()
    with open("./result.txt", "w") as fp:
        fp.write("\n".join([str(k) for k in result[ :10]]))
    sc.stop()
    print("success")

if __name__ == '__main__':
    kafka_consumer()