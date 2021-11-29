#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@文件        :test01.py
@时间        :2021/10/17 17:25:49
@作者        :weiqs
@邮箱        :weiqs@asiainfo.com
@版本        :1.0
@说明        :

'''

from tqdm import tqdm
import time
import json
import re
from simhash import Simhash

def test01():
    # 拆分数据集
    cnt = 0
    # Total : 1077394
    train_data = []
    test_data = []
    error_data = []
    split_date = time.strptime("2021-07-31 00:00:01", "%Y-%m-%d %H:%M:%S")
    split_time = time.mktime(split_date)

    with open("./data/2021_08_25.log", "r") as fp:
        for line in tqdm(fp, desc="load data ... "):
            try:
                d = json.loads(line.strip())
                timestamp = d["timestamp"] // 1000
                if timestamp > split_time:
                    test_data.append(line)
                else:
                    train_data.append(line)
            except:
                error_data.append(line)
           
            # # print(timestamp)
            # timeArray = time.localtime(timestamp)
            # otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
            # d["timestamp"] = otherStyleTime
            # if otherStyleTime > "2021-07-31 00:00:01":
            #     test_data.append(str(json.dumps(d, ensure_ascii=False)))
            # else:
            #     train_data.append(str(json.dumps(d, ensure_ascii=False)))
            cnt += 1
            # break
    
    with open("./data/train_data.txt", "w", encoding="utf-8") as fw:
        fw.write("".join(train_data))
    
    with open("./data/test_data.txt", "w", encoding="utf-8") as fw:
        fw.write("".join(test_data))

    with open("./data/error_data.txt", "w", encoding="utf-8") as fw:
        fw.write("".join(error_data))

    # train data 460392
    # test data 590786

def parse_func(s):
    f = re.findall("[._a-zA-Z0-9\u4e00-\u9fa5]+", s)
    return f

def test02():
    # 抽取训练集关键词
    word_count = {}
    cnt = 0
    with open("./data/train_data.txt", "r") as fp:
        for line in tqdm(fp, desc="load data ... "):
            d = json.loads(line.strip(), strict=False)
            body = d["body"].strip()
            arr = parse_func(body)
            cnt += 1
            for w in set(arr):
                word_count[w] = word_count.get(w, 0) + 1
    # print(first_character)
    # sort_items = sorted(word_count.items(), key=lambda x: x[1], reverse=True)
    # with open("./data/words_list.txt", "w", encoding="utf-8") as fp:
    #     fp.write("\n".join([str(v) for v in sort_items]))
    # print(cnt)
    with open("./data/words.json", "w", encoding="utf-8") as g:
        g.write(json.dumps(word_count, ensure_ascii=False))


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


def test03():
    # 提取模板词
    template_words = {}
    with open("./data/words.json", 'r') as fp:
        words = json.load(fp)
        for k, v in words.items():
            if v < 1 + 460392 * 0.001:
                continue
            elif check_rule(k):
                continue
            else:
                template_words[k] = v
    print(len(template_words))
    with open("./data/template_words.json", "w", encoding="utf-8") as g:
        g.write(json.dumps(template_words, ensure_ascii=False))

def format_line(arr, template_words):
    res = []
    for k in arr:
        if k in template_words:
            res.append(k)
        else:
            if len(res) == 0:
                res.append("*")
            elif res[-1] == "*":
                continue
            else:
                res.append("*")
    return res

def test04():
    # 通配符替换
    with open("./data/template_words.json", 'r') as fp:
        words = json.load(fp)

    index_count = {}
    cnt = 0
    with open("./data/train_data.txt", "r") as fp:
        for line in tqdm(fp, desc="load data ... "):
            d = eval(line.strip())
            body = d["body"].strip()
            arr = parse_func(body)
            formart_arr = format_line(arr, words)
            hash_value = Simhash(formart_arr).value
            info = index_count.get(hash_value, {})
            info["cnt"] = info.get("cnt", 0) + 1
            info["seq"] = " ".join(formart_arr)
            index_count[hash_value] = info
            cnt += 1
    # sorted_items = sorted(index_count.items(), key=lambda x: x[1]["cnt"], reverse=True)
    print(len(index_count))
    with open("./data/index_counts.json", "w", encoding="utf-8") as g:
        g.write(json.dumps(index_count, ensure_ascii=False))

# def test04():
#     # 通配符替换
#     with open("./data/template_words.json", 'r') as fp:
#         words = json.load(fp)

#     format_lines = []
#     cnt = 0
#     with open("./data/train_data.txt", "r") as fp:
#         for line in tqdm(fp, desc="load data ... "):
#             d = eval(line.strip())
#             body = d["body"].strip()
#             arr = parse_func(body)
#             formart_arr = [k if k in words else "*" for k in arr]
#             if formart_arr[0] == "*":
#                 continue
#             format_lines.append(" ".join(formart_arr))
#             cnt += 1
#     with open("./data/format_train_data.txt", "w",encoding="utf-8") as fp:
#         fp.write("\n".join(format_lines))

# def test05():
#     # 获取模板中心, 23385
#     index_count = {}
#     cnt = 0
#     with open("./data/format_train_data.txt", "r") as fp:
#         for line in tqdm(fp, desc="load data ... "):
#             formart_arr = line.strip().split(" ")
#             hash_value = Simhash(formart_arr).value
#             info = index_count.get(hash_value, {})
#             info["cnt"] = info.get("cnt", 0) + 1
#             info["seq"] = line.strip()
#             index_count[hash_value] = info
#             cnt += 1
#     with open("./data/index_counts.json", "w", encoding="utf-8") as g:
#         g.write(json.dumps(index_count, ensure_ascii=False))

def test06():
    # 计算编辑距离，合并模板 
    cnt = 0
    with open("./data/index_counts.json", "r") as fp:
        index_count = json.load(fp)

    print(len(index_count))

    # result = {}
    # visited = set()
    # sorted_items = sorted(index_count.items(), key=lambda x: x[1]["cnt"], reverse=True)
    # for i in tqdm(range(len(sorted_items)), desc="load data ..."):
    #     x = sorted_items[i]
    #     x_head = x[1]["seq"].split(" ")[0]
    #     if x[0] in visited:
    #         continue
    #     result[x[0]] = result.get(x[0], [])
    #     result[x[0]].append(x)
    #     visited.add(x[0])
    #     for j in range(1, len(sorted_items)):
    #         y = sorted_items[j]
    #         y_head = y[1]["seq"].split(" ")[0]
    #         if x_head == y_head and y[0] not in visited:
    #             sim_x = Simhash(int(x[0]))
    #             sim_y = Simhash(int(y[0]))
    #             d = sim_x.distance(sim_y)
    #             if d <= 3:
    #                 result[x[0]].append(y)
    #                 visited.add(y[0])

    # with open("./data/result.json", "w", encoding="utf-8") as g:
    #     g.write(json.dumps(result, ensure_ascii=False))


def format_check_line(arr, pos=10):
    res = []
    for k in arr:
        if len(res) == 0:
            v = "*" if check_rule(k) else k
            res.append(v)
        elif len(res) >= pos:
            break
        else:
            v = "*" if check_rule(k) else k
            if res[-1] == "*" and v == "*":
                continue
            else:
                res.append(v)

    return res


def test08():
    # 加载测试数据，检测日志模板
    cnt = 0
    with open("./data/severityText.json", "r") as fp:
        severityText = json.load(fp)
    with open("./data/resource.json", "r") as fp:
        resource = json.load(fp)
    with open("./data/index_counts.json", "r") as fp:
        index_count = json.load(fp)
    with open("./data/template_words.json", 'r') as fp:
        words = json.load(fp)

    error_logs = {}

    cnt = 0
    # 2021-08-07 2021_10_14.log
    # total 2021_08_25.log 
    error_msg = []
    with open("./data/test.log", "r") as fp:
        for line in tqdm(fp, desc="load data ... "):
            try:
                d = json.loads(line.strip(), strict=False)
            except:
                error_msg.append(line)
                continue
            severity_value = d["severityText"]
            resource_value = d["resource"]["name"]
            body = d["body"].strip()
            arr = parse_func(body)
            formart_arr = format_line(arr, words)
            hash_value = Simhash(formart_arr).value

            if len(formart_arr) == 1 and formart_arr[0] == "*":
                formart_arr = format_check_line(arr)
                hash_value = Simhash(formart_arr).value

            if severityText.get(severity_value, 0) < 10:
                error_logs[hash_value] = error_logs.get(hash_value, {})
                error_logs[hash_value]["cnt"] = error_logs[hash_value].get("cnt", 0) + 1
                error_logs[hash_value]["seq"] = error_logs[hash_value].get("seq", [])
                error_logs[hash_value]["seq"].append(str(json.dumps(d, ensure_ascii=False)))
                error_logs[hash_value]["template"] = " ".join(formart_arr)
                cnt += 1
            if resource.get(resource_value, 0) < 50:
                error_logs[hash_value] = error_logs.get(hash_value, {})
                error_logs[hash_value]["cnt"] = error_logs[hash_value].get("cnt", 0) + 1
                error_logs[hash_value]["seq"] = error_logs[hash_value].get("seq", [])
                error_logs[hash_value]["seq"].append(str(json.dumps(d, ensure_ascii=False)))
                error_logs[hash_value]["template"] = " ".join(formart_arr)
                cnt += 1

            t = index_count.get(str(hash_value), {}).get("cnt", 0)
            if t < 20:
                error_logs[hash_value] = error_logs.get(hash_value, {})
                error_logs[hash_value]["cnt"] = error_logs[hash_value].get("cnt", 0) + 1
                error_logs[hash_value]["seq"] = error_logs[hash_value].get("seq", [])
                error_logs[hash_value]["seq"].append(str(json.dumps(d, ensure_ascii=False)))
                error_logs[hash_value]["template"] = " ".join(formart_arr)
                cnt += 1

    print("Total error logs: %d" % len(error_logs))
    print("Total Content: %d" % cnt)
    print("Error message: %d" % len(error_msg))

    with open("./data/test_error_logs.json", "w", encoding="utf-8") as g:
        g.write(json.dumps(error_logs, ensure_ascii=False))

    with open("./data/error_message.txt", "w", encoding="utf-8") as fw:
        fw.write("".join(error_msg))

def test08_1():
    # 加载测试数据，提取模板数量
    cnt = 0
    with open("./data/severityText.json", "r") as fp:
        severityText = json.load(fp)
    with open("./data/resource.json", "r") as fp:
        resource = json.load(fp)
    with open("./data/index_counts.json", "r") as fp:
        index_count = json.load(fp)
    with open("./data/template_words.json", 'r') as fp:
        words = json.load(fp)

    # error_logs = {}

    # cnt = 0
    # 2021-08-07 2021_10_14.log
    # total 2021_08_25.log 
    # error_msg = []
    template_count = {}

    with open("./data/all.log", "r") as fp:
        for line in tqdm(fp, desc="load data ... "):
            try:
                d = json.loads(line.strip())
            except:
                # error_msg.append(line)
                continue
            body = d["body"].strip()
            arr = parse_func(body)
            formart_arr = format_line(arr, words)
            hash_value = Simhash(formart_arr).value
            t = index_count.get(str(hash_value), {}).get("cnt", 0)
            if t > 1:
                template_count[hash_value] = template_count.get(hash_value, [])
                template_count[hash_value].append(d["timestamp"])
            

    with open("./data/template_counts.json", "w", encoding="utf-8") as g:
        g.write(json.dumps(template_count, ensure_ascii=False))
    
def test09():
    # 结果分析
    with open("./data/test_error_logs.json", 'r') as fp:
        error_logs = json.load(fp)

    print(len(error_logs))
    s = 0
    sub_list = []
    for k, v in error_logs.items():
        s += v.get("cnt", 0)
        sub_list.append(str(json.dumps({"templateID": k, "cnt": v.get("cnt", 0), "seq":eval(v["seq"][0])})))

    print("Total: %d" % s)
    
    with open("./data/test_error_logs_sample.txt", "w", encoding="utf-8") as fw:
        fw.write("\n".join(sub_list))

def test10():
    # 结果分析
    with open("./data/test_error_logs_total.json", 'r') as fp:
        error_logs = json.load(fp)

    s = 0
    time_count = {}
    for k, v in error_logs.items():
        s += v.get("cnt", 0)
        for seq in v.get("seq", []):
            d = json.loads(seq)
            timestamp = d["timestamp"] // 1000
            timeArray = time.localtime(timestamp)
            otherStyleTime = time.strftime("%Y-%m-%d %H", timeArray)
            time_count[otherStyleTime] = time_count.get(otherStyleTime, {})
            time_count[otherStyleTime]["cnt"] = time_count[otherStyleTime].get("cnt", 0) + 1
            time_count[otherStyleTime]["type"] = time_count[otherStyleTime].get("type", set())
            time_count[otherStyleTime]["type"].add(k)
            
    print("Total: %d" % s)

    sort_items = sorted(time_count.items(), key=lambda x: x[1]["cnt"], reverse=True)
    with open("./data/test_error_time_sample.txt", "w", encoding="utf-8") as fw:
        fw.write("\n".join([str(v) for v in sort_items]))


def test11():
    # 异常检测结果时序合并

    with open("./data/test_error_logs.json", "r", encoding="utf-8") as fp:
        totoal_error = json.load(fp)

    # with open("./data/test_error_logs_0807.json", "r", encoding="utf-8") as fp:
    #     error_0807 = json.load(fp)

    time_count = {}
    for k, v in totoal_error.items():
        for seq in v.get("seq", []):
            d = json.loads(seq)
            timestamp = d["timestamp"] // 1000
            timeArray = time.localtime(timestamp)
            otherStyleTime = time.strftime("%Y-%m-%d %H:%M", timeArray)
            time_count[otherStyleTime] = time_count.get(otherStyleTime, 0) + 1
            # time_count[otherStyleTime] = time_count.get(otherStyleTime, {})
            # time_count[otherStyleTime]["cnt"] = time_count[otherStyleTime].get("cnt", 0) + 1
            # time_count[otherStyleTime]["type"] = time_count[otherStyleTime].get("type", set())
            # time_count[otherStyleTime]["type"].add(k)

    with open("./data/time_series.json", "w", encoding="utf-8") as g:
        g.write(json.dumps(time_count, ensure_ascii=False))


def test12():
    # 异常日志关键词统计
    with open("./data/test_error_logs.json", "r", encoding="utf-8") as fp:
        totoal_error = json.load(fp)

    with open("./data/template_words.json", 'r') as fp:
        words = json.load(fp)
    
    words_count = {}
    for k, v in totoal_error.items():
        for seq in v.get("seq", []):
            d = json.loads(seq)
            body = d["body"].strip()
            arr = parse_func(body)
            for w in arr:
                if w not in words:
                    words_count[w] = words_count.get(w, 0) + 1
    
    for k in list(words_count.keys()):
        if words_count[k] < 5:
            words_count.pop(k)

    with open("./data/error_words_count.json", "w", encoding="utf-8") as g:
        g.write(json.dumps(words_count, ensure_ascii=False))


def test13():
    with open("./data/words.json", 'r') as fp:
        words = json.load(fp)
        for k, v in words.items():
            p = r'.*拒绝.*'
            if re.match(p, k) is not None:
                print(k, v)

    
if __name__ == '__main__':
    # test03()

    with open("./data/template_words.json", 'r') as fp:
        words = json.load(fp)

    s = '{"timestamp":1627663263763,"attributes":{"organization":"ah","logType":"LsmsSvc","logTypeName":"携号转网","logPath":"/toptea/logstash_data/log_file/test_only_one_tmp/LsmsSvc01_2021-07-31.log"},"resource":{"objType":"log","name":" c.s.c.l.a.s.CallService"},"severityText":"INFO","body":"TransUtils.getSendUrl(transMsg.getCmdCode()) = http://10.254.54.252:3913/v1/Prov/soa?province=551"}'
    # s = "^"
    # body = json.loads(s)["body"]
    body = "123123123"
    reg = "[._a-zA-Z0-9\u4e00-\u9fa5]+"
    a = re.finditer(reg, body)
    sub_list = []
    pos = 0
    for k in a:
        if k.group() not in words:
            start_pos, end_pos = k.span()
            sub_list.append(body[pos: start_pos])
            sub_list.append("<span>%s</span>" % k.group())
            pos = end_pos
    sub_list.append(body[pos: ])
    print("".join(sub_list))