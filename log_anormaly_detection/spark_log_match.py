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

def load_multiple_models(path):
    with open(os.path.join(path, "index_counts.json"), "r") as fp:
        index_count = json.load(fp)
    with open(os.path.join(path, "template_words.json"), 'r') as fp:
        words = json.load(fp)

    return {
        "word_index": words,
        "seq_index" : index_count
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

    word_index = models["word_index"]
    seq_index = models["seq_index"]
    log_body = log_content["body"].strip()
    res_name = log_content["resource"]["name"]
    log_type = log_content["attributes"]["logType"]
    organization = log_content["attributes"]["organization"]
    log_time = log_content["timestamp"]

    arr = parse_func(log_body)
    formart_arr = format_line(arr, word_index)
    hash_value = Simhash(formart_arr).value
    t = seq_index.get(str(hash_value), {}).get("cnt", 0)
    res = {
        "logObject": res_name,
        "logType": log_type,
        "organization": organization,
        "logTime": log_time,
        "logMessage": log_body,
        "logMessageRegularization": " ".join(formart_arr),
        "logTemplateId": str(hash_value),
        "logDetail": log_body,
        "logAnomalyId" : ""
    }
    if t < 2:
        res["logTemplateId"] = "F"

    return res

def validate_json(raw):
    try:
        json.loads(raw)
    except ValueError as err:
        return False
    return True


if __name__ == '__main__':

    brokers = sys.argv[2]
    topic = sys.argv[3]
    topic_out = sys.argv[4]
    batch_interval = 10

    conf = SparkConf().set("spark.python.profile", "true").set(
        "spark.io.compression.codec", "snappy")  # .setMaster('local[*]')
    # conf.setAppName('spark-test')
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, batch_interval)
    kafkaStreams = KafkaUtils.createDirectStream(
        ssc, [topic], kafkaParams={"metadata.broker.list": brokers})

    # broadcase models to worker nodes
    model = load_multiple_models(sys.argv[1])
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
