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

import os, sys
current_dir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(current_dir)

def load_multiple_models(path):
    models = {}
    if os.path.exists(path):
        for model_folder in os.listdir(path):
            try:
                models[model_folder] = load_single_models(os.path.join(path, model_folder))
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
def send_msg(msg):
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
    model = models.get(model_id, None)

    log_body = log_content["body"].strip()
    log_time = log_content["timestamp"]

    res = {
        "logObject": res_name,
        "logType": log_type,
        "organization": organization,
        "logTime": log_time,
        "logMessage": log_body,
        "logMessageRegularization": "",
        "logTemplateId": "F",
        "logDetail": ""
    }
    if model is None:
        res["logDetail"] = "attributes_organization_logType is null"
        return res
    else:
        word_index = model.get("word_index", None)
        seq_index = model.get("seq_index", None)
        if word_index is None or seq_index is None:
            res["logDetail"] = "template is None"
            return res
        arr = parse_func(log_body)
        formart_arr = format_line(arr, word_index)
        code = Simhash(formart_arr).value
        hex_code = (str(hex(code))[2: ]).upper()
        fill_code = ("%4s" % hex_code).replace(" ", "0")
        final_code = "E" + fill_code
        t = seq_index.get(final_code, 0)
        res["logMessageRegularization"] = " ".join(formart_arr)
        res["logTemplateId"] = final_code
        res["logDetail"] = "code is %s, count is 0" % final_code
        if t == 0:
            res["logTemplateId"] = "F"
            res["logDetail"] = "code is %s, count is 0" % final_code 
        return res

def validate_json(raw):
    try:
        json.loads(raw, strict=False)
    except ValueError as err:
        return False
    return True


if __name__ == '__main__':
    
    ftp_path = sys.argv[1]
    ftp_config = json.loads(sys.argv[2])
    brokers = sys.argv[3]
    topic = sys.argv[4]
    topic_out = sys.argv[5]

    pkl_files = os.path.join(current_dir, "pkl_files")
    download_files(ftp_config, ftp_path, pkl_files)
    model = load_multiple_models(pkl_files)
    
    batch_interval = 10

    conf = SparkConf().set("spark.python.profile", "true").set(
        "spark.io.compression.codec", "snappy")  # .setMaster('local[*]')
    # conf.setAppName('spark-test')
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, batch_interval)
    kafkaStreams = KafkaUtils.createDirectStream(
        ssc, [topic], kafkaParams={"metadata.broker.list": brokers})

    # broadcase models to worker nodes
    brodcast_model = sc.broadcast(model)
    # Producer to write result backto kafka
    producer = KafkaProducer(bootstrap_servers=brokers,
                             value_serializer=lambda m: json.dumps(m).encode('utf-8'))

    # Get content
    log_content = kafkaStreams.filter(lambda m: validate_json(
        m[1])).map(lambda m: json.loads(m[1], strict=False))
    # message match template
    log_predict = log_content.map(
        lambda m: log_process(brodcast_model.value, m))
    # result to kafka
    log_predict.foreachRDD(send_msg)

    ssc.start()
    ssc.awaitTermination()
    # logger_obj.removeHandler(logger_obj.handlers)
