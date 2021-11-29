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
import json
import re
from simhash import Simhash, SimhashIndex

def test01():
    severityText_value = {}
    resource_value = {}
    cnt = 0
    # Total : 1077394
    with open("./data/train_data.txt", "r") as fp:
        for line in tqdm(fp, desc="load data ... "):
            d = eval(line.strip())
            severityText_value[d["severityText"]] = severityText_value.get(d["severityText"], 0) + 1
            resource_value[d["resource"]["name"]] = resource_value.get(d["resource"]["name"], 0) + 1
            cnt += 1
    print(cnt)
    print(severityText_value)
    print(resource_value)

    with open("./data/severityText.json", "w", encoding="utf-8") as g:
        g.write(json.dumps(severityText_value, ensure_ascii=False))

    with open("./data/resource.json", "w", encoding="utf-8") as g:
        g.write(json.dumps(resource_value, ensure_ascii=False))

def getLCS(s1, s2):
    chars = []
    for i in range(len(s1)):
        if s1[i] == s2[i]:
            chars.append(s1[i])
        else:
            break
    return "".join(chars)

def test02():

    # first_character = {}
    body_head = {}
    # Total : 1077394
    cnt = 0
    with open("./data/07to09-LsmsSvd.log", "r") as fp:
        for line in tqdm(fp, desc="load data ... "):
            d = eval(line.strip())
            body = d["body"].strip()
            match = re.match("[a-zA-Z\u4e00-\u9fa5]+", body)
            if match is None:
                print(body)
            else:
                first_alpha = match.group(0)
                body_head[first_alpha] = body_head.get(first_alpha, 0) + 1
            cnt += 1
            # if cnt >= 1000:
            #     break

    # print(first_character)
    with open("bodyHead.json", "w", encoding="utf-8") as g:
        g.write(json.dumps(body_head, ensure_ascii=False))

def test03():

    # 接收CRM的请求
    # 由CRM发起调用集团
    # 反馈CRM的应答
    # 接收集团的请求
    # 调用ESBinDto
    # ESB返回retJson

    idx_list = [
        "接收CRM的请求", "由CRM发起调用集团", "反馈CRM的应答",
        "接收集团的请求", "调用ESBinDto", "ESB返回retJson"
    ]
    # first_character = {}
    # body_head = {}
    # Total : 1077394
    step_content = []
    cnt = 0
    idx = 5
    with open("./data/07to09-LsmsSvd.log", "r") as fp:
        for line in tqdm(fp, desc="load data ... "):
            d = eval(line.strip())
            body = d["body"].strip()
            if body.startswith(idx_list[idx]):
                content = {
                    "body": body,
                    "timestamp": d["timestamp"]
                }
                step_content.append(json.dumps(content, ensure_ascii=False))
            cnt += 1
            # if cnt >= 1000:
            #     break

    # print(first_character)
    with open("./data/step_%02d.txt" % (idx + 1), "w", encoding="utf-8") as g:
        g.write("\n".join(step_content))


def test04():
    # 提取 message ID
    step_content = []
    cnt = 0
    idx = 5
    with open("./data/step_%02d.txt" % (idx + 1), "r") as fp:
        for line in tqdm(fp, desc="load data ... "):
            j = json.loads(line.strip())
            content = {
                "timestamp": j["timestamp"]
            }
            body = j["body"]
            message_id = ""
            ##### step_01 
            # body = body.replace("'", "")
            # arr = body.split(",")
            # message_id = ""
            # for v in arr:
            #     if v.startswith("MESSAGE_ID"):
            #         message_id = v.split(":")[1]
            ###### step_02
            # start_index = body.index("<MessageID>") + len("<MessageID>")
            # end_index = body.index("</MessageID>")
            # message_id = body[start_index: end_index]
            # content["message_id"] = message_id
            # step_content.append(json.dumps(content, ensure_ascii=False))
            ###### step_03
            # title = "反馈CRM的应答：inputrestPubCallresponseBody["
            # start_index = body.index(title) + len(title)
            # temp_dict = eval(body[start_index: -1])
            # content["message_id"] = temp_dict["ROOT"]["BODY"]["OUT_DATA"]["MESSAGE_ID"]
            # content["body"] = temp_dict["ROOT"]["BODY"]
            ###### step_04
            # start_index = body.index("<urn:MessageID>") + len("<urn:MessageID>")
            # end_index = body.index("</urn:MessageID>")
            # content["message_id"] = body[start_index: end_index]
            # content["orig_message_id"] = ""
            # content["response_code"] = ""
            # if body.find("<urn:OrigMessageID>") > 0:
            #     start_index = body.index("<urn:OrigMessageID>") + len("<urn:OrigMessageID>")
            #     end_index = body.index("</urn:OrigMessageID>")
            #     content["orig_message_id"] = body[start_index: end_index]
            # if body.find("<urn:ResponseCode>") > 0:
            #     start_index = body.index("<urn:ResponseCode>") + len("<urn:ResponseCode>")
            #     end_index = body.index("</urn:ResponseCode>")
            #     content["response_code"] = body[start_index: end_index]

            ###### step_05
            # body = body.replace("null", "''")
            # title = "调用ESBinDto="
            # start_index = body.index(title) + len(title)
            # temp_dict = eval(body[start_index: ])
            # # print(json.dumps(temp_dict, ensure_ascii=False))
            # message_id = temp_dict["ROOT"]["BODY"]["BUSI_INFO"].get("MESSAGE_ID", "")
            # content["message_id"] = message_id
            # content["body"] = temp_dict["ROOT"]["BODY"]["BUSI_INFO"]

            ###### step_06
            title = "ESB返回retJson="
            start_index = body.index(title) + len(title)
            try:
                temp_dict = eval(body[start_index: ])
                # print(json.dumps(temp_dict, ensure_ascii=False))
                message_id = temp_dict["ROOT"]["BODY"]["OUT_DATA"].get("MESSAGE_ID", "")
                content["message_id"] = message_id
                content["body"] = temp_dict["ROOT"]["BODY"]
            except:
                print(body)
            step_content.append(json.dumps(content, ensure_ascii=False))
            cnt += 1
            # if cnt >= 1:
            #     print(step_content)
            #     break

    with open("./data/step_info_%02d.txt" % (idx + 1), "w", encoding="utf-8") as g:
        g.write("\n".join(step_content))

def parse_func(s):
    f = re.findall("[.a-zA-Z0-9\u4e00-\u9fa5]+", s)
    return f

def jaccard_score(s1, s2):
    if s1[0] == s2[0] and len(s1) == len(s2):
        return len(set(s1) & set(s2)) / len(set(s1) | set(s2))
    else:
        return 0

def test05():
    idx = 0
    cnt = 0
    seed = None
    score_list = []
    with open("./data/step_%02d.txt" % (idx + 1), "r") as fp:
        for line in tqdm(fp, desc="load data ... "):
            j = json.loads(line.strip())
            body = j["body"]
            # print(body)
            arr = parse_func(body)
            if seed is None:
                seed = arr
            else:
                score = jaccard_score(seed, arr)
                score_list.append(score)
            cnt += 1
            if cnt > 50:
                break
    print(score_list)


def test06():
    # 抽取日志中所有的词，构建词典
    word_count = {}
    cnt = 0
    with open("./data/07to09-LsmsSvd.log", "r") as fp:
        for line in tqdm(fp, desc="load data ... "):
            d = eval(line.strip())
            body = d["body"].strip()
            arr = parse_func(body)
            visited = set()
            for w in arr:
                if w not in visited:
                    word_count[w] = word_count.get(w, 0) + 1
                    visited.add(w)
    # print(first_character)
    with open("./data/words.json", "w", encoding="utf-8") as g:
        g.write(json.dumps(word_count, ensure_ascii=False))

def test07():
    # 提取模板词
    template_words = {}
    with open("./data/words.json", 'r') as fp:
        words = json.load(fp)
        for k, v in words.items():
            if v > 1000:
                template_words[k] = v
    with open("./data/template_words.json", "w", encoding="utf-8") as g:
        g.write(json.dumps(template_words, ensure_ascii=False))

def test08():
    with open("./data/template_words.json", 'r') as fp:
        words = json.load(fp)

    format_lines = []
    cnt = 0
    with open("./data/07to09-LsmsSvd.log", "r") as fp:
        for line in tqdm(fp, desc="load data ... "):
            d = eval(line.strip())
            body = d["body"].strip()
            arr = parse_func(body)
            formart_arr = [k if k in words else "*" for k in arr]
            if formart_arr[0] == "*":
                continue
            format_lines.append(" ".join(formart_arr))
            cnt += 1
    with open("./data/format.log", "w",encoding="utf-8") as fp:
        fp.write("\n".join(format_lines))

def test09():

    seed_seq = []
    index_count = {}

    cnt = 0
    with open("./data/format.log", "r") as fp:
        for line in tqdm(fp, desc="load data ... "):
            formart_arr = line.strip().split(" ")
            hash_value = Simhash(formart_arr).value
            info = index_count.get(hash_value, {})
            info["cnt"] = info.get("cnt", 0) + 1
            info["seq"] = line.strip()
            index_count[hash_value] = info
            # if len(seed_seq) == 0:
            #     seed_seq.append(formart_arr)
            # else:
            #     flag = True
            #     for i, s in enumerate(seed_seq):
            #         score = jaccard_score(s, formart_arr)
            #         if score > 0.9:
            #             index_count[i] = index_count.get(i, 0) + 1
            #             flag = False
            #             continue
            #     if flag:
            #         seed_seq.append(formart_arr)
            cnt += 1
            # if cnt > 50:
            #     break
    # print(seed_seq)

    # with open("./data/template_seq.txt", "w",encoding="utf-8") as fp:
    #     fp.write("\n".join([ " ".join(s) for s in seed_seq]))
    
    with open("./data/index_counts.json", "w", encoding="utf-8") as g:
        g.write(json.dumps(index_count, ensure_ascii=False))

def test10():
    with open("./data/index_counts.json", 'r') as fp:
        index_count = json.load(fp)
    print(len(index_count))
    cnt_list = []
    for k, v in index_count.items():
        cnt_list.append(v["cnt"])
        if v["cnt"] > 1000:
            print(v["seq"])

    print(len(cnt_list))
    print(sum(cnt_list))
    print(cnt_list)
    
    # with open("./data/template_seq.txt", 'r') as fp:
    #     seq = [line.strip() for line in fp]
    
    # t = 0
    # for k, v in index_count.items():
    #     t += v
    # print(t)

    # for i, s in enumerate(seq):
    #     if index_count.get(str(i), 0) > 10000:
    #         print(s)


def insert_node(d, arr):
    for k in arr:
        _cnt = d.get(k, {"_cnt": 0}).get("_cnt") + 1
        _next = d.get(k, {"_next": {}})

    pass


def test11():
    temp_list = []
    cnt = 0
    with open("./data/format.log", "r") as fp:
        for line in tqdm(fp, desc="load data ... "):
            format_arr = line.strip().split(" ")
            key = format_arr[0]
            if key == "由CRM发起调用集团" :
                temp_list.append(line)
            cnt += 1
            # if cnt > 50:
            #     break
    # print(seed_seq)

    with open("./data/temp_01.txt", "w",encoding="utf-8") as fp:
        fp.write("".join(temp_list))

if __name__ == '__main__':
    test01()
    # s = '"name":"12312312312321312321X"'
    # a = re.findall("[.a-zA-Z0-9]+", s)
    # print(1)
