# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch, helpers
import time
import sys
from tqdm import tqdm

host = sys.argv[1]
port = sys.argv[2]
index_name = sys.argv[3]
doc_type = sys.argv[4]

#host = "10.19.90.9"
#port = "9200"
#index_name = "training_result_index"
#doc_type = "training_result"

# 批量插入es记录,建议每批1万条以内
def bluk(es, index_name, doc_type, list):
    # es = Elasticsearch([host, port])
    s = time.time()
    actions = []
    for item in tqdm(list, "插入数据..."):
        # 拼接插入数据结构
        action = {
            "_index": index_name,
            "_type": doc_type,
            "_source": item
        }
        # 形成一个长度与查询结果数量相等的列表
        actions.append(action)
        # 批量插入
    a = helpers.bulk(es, actions)
    e = time.time()
    print("bluk end ,成功入库: 总条数{},花费时间: {} s".format(a, e - s))


def query():
    es = Elasticsearch(hosts="http://" + host + ":" + port + "/")
    query_json = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "gathTime": {
                                "gt": "2021-11-24 03:00:00",
                                "lt": "2021-11-24 11:00:00"
                            }
                        }
                    }
                ]
            }
        }
    }

    # query = es.search(index='training_result_index', body=query_json)
    query = es.search(index=index_name, body=query_json, scroll='5m', size=100)

    results = query['hits']['hits']  # es查询出的结果第一页
    total = query['hits']['total']  # es查询出的结果总量
    scroll_id = query['_scroll_id']  # 游标用于输出es查询出的所有结果

    for i in range(0, int(total / 100) + 1):
        # scroll参数必须指定否则会报错
        query_scroll = es.scroll(scroll_id=scroll_id, scroll='5m')['hits']['hits']
        # results += query_scroll

        print(len(query_scroll))
        list = []
        for res in tqdm(query_scroll, desc="part-%d 读取数据..." % i):
            # newJson=res['_source'].replace('2021','2018').replace("'",'"')
            dict=res['_source']
            newDate = dict['gathTime'].replace('2021-11-24', '2021-11-25')
            dict['gathTime']=newDate
            list.append(dict)
            # print(dict)
        bluk(es, index_name,doc_type,list)


if __name__ == '__main__':
    query()
