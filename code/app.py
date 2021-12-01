import sys
import pandas as pd
# pd.options.display.max_columns = None
pd.options.display.max_colwidth = None
import re
from tqdm import tqdm
import hashlib
import json
import numpy as np
import time, datetime
import os

  

# train_path = 'datasets/train'
# result_path = 'results'
# test_path = 'datasets/test'
train_path = sys.argv[1]
result_path = sys.argv[2]
test_path = sys.argv[3]
# print(train_path)
# print(result_path)
# print(test_path)


test_list = []
with open(os.path.join(test_path, "messages_testset.txt"),'r',encoding='utf-8') as f:
    for i,line in enumerate(f.readlines()):
        try:
            timestring = line.split(maxsplit=1)[0].split(sep="|",maxsplit=1)[0]
            if len(timestring)>35:
                timestring = timestring.replace('\x00','')
#                 print(i)
#                 print(timestring)
#                 print(len(timestring))
            _timestring = timestring[:19]
            timeArray = time.strptime(_timestring, "%Y-%m-%dT%H:%M:%S")
            timestamp = int(time.mktime(timeArray) // 300)

#             _message = line[len(timestring)+1:].strip()
#             index_1 = _message.find('|')
#             index_2 = _message.find(':')
#             if (index_1 != -1) & (index_2 >= index_1):
#                 reason, message = _message.split(sep="|",maxsplit=1)[:]
#                 message = message.strip()

#             else:
#                 reason, message = _message.split(sep=":",maxsplit=1)[:]
#                 reason = reason.split()[-1].strip()
#                 message = message.strip()
#             test_list.append([i, timestamp, reason, message, line]) 
        except:
#             print(i)
#             print(line)
#             print()
            continue
    
        _message = line[len(timestring)+1:].strip()
        index_1 = _message.find('|')
        index_2 = _message.find(':')
        if ((index_1 != -1) & (index_2 >= index_1)) | (index_1 != -1):
            reason, message = _message.split(sep="|",maxsplit=1)[:]
            message = message.strip()

        else:
            reason, message = _message.split(sep=":",maxsplit=1)[:]
            reason = reason.split()[-1].strip()
            message = message.strip()
        test_list.append([i+1, timestamp, reason, message, line]) 
    
df_test = pd.DataFrame(test_list,columns=['LineId','timestamp','reason','message','line'])
# print(df_test.head())
print(df_test.shape)



pattern = r"lose cover|disabled|disabling|interrupt"
pattern = re.compile(pattern, re.M|re.I)
def label_message_test(x):
    matchObj = re.search(pattern, x)
    if matchObj:
        return 1
    else:
        return 0


df_test['Label'] = df_test['message'].apply(label_message_test)

df_test_1 = df_test[['timestamp','Label']]
df_test_1.columns = ['TimeSlice','Label']
df_test_1['LogName'] = 'messages'
df_test_1 = df_test_1[['LogName', 'TimeSlice', 'Label']]


sys_test_list = []
with open(os.path.join(test_path, "sysmonitor_testset.txt"),'r',encoding='utf-8') as f:
    for i,line in enumerate(tqdm(f.readlines())):
        try:
            timestring = line.split(maxsplit=1)[0].split(sep="|",maxsplit=1)[0]
            _timestring = timestring[:19]
            timeArray = time.strptime(_timestring, "%Y-%m-%dT%H:%M:%S")
            timestamp = int(time.mktime(timeArray) // 300)

        except:
            continue
            
        _message = line[len(timestring)+1:].strip()
        index_1 = _message.find('|')
        index_2 = _message.find(':')
        if ((index_1 != -1) & (index_2 >= index_1)) | (index_1 != -1):
            reason, message = _message.split(sep="|",maxsplit=1)[:]
            message = message.strip()

        else:
            reason, message = _message.split(sep=":",maxsplit=1)[:]
            reason = reason.split()[-1].strip()
            message = message.strip()
        sys_test_list.append([i+1, timestamp, reason, message, line]) 
df_sys_test = pd.DataFrame(sys_test_list,columns=['LineId','timestamp','reason','message','line'])
# print(df_sys_test.head())
print(df_sys_test.shape)


# pattern_sys = r"alarm|abnormal"
# pattern_sys = r"(alarm)*.*(memory)"
pattern_sys = r".*((alarm.*memory)|(memory.*alarm)|(resum.*alarm)|(resum.*alarm)).*"
pattern_sys = re.compile(pattern_sys, re.M|re.I)
def label_message_sys(x):
    matchObj = re.search(pattern_sys, x)
#     if matchObj and 'memory' in x:
    if matchObj:
        return 1
    else:
        return 0
    
df_sys_test['Label'] = df_sys_test['message'].apply(label_message_sys)

# def anomaly_candidates_sys(row):
#     if row['anomaly_candidates'] == 1:
#         return 1
#     else:
#         return row['Label']

# df_sys_anomaly = pd.read_csv('sys_execute_path_anormal.csv')
# sys_anomaly_ids = df_sys_anomaly.LineId.values
# df_sys_test['anomaly_candidates'] = 0
# for idx in sys_anomaly_ids:
#     i = df_sys_test[df_sys_test.LineId == idx].index
#     df_sys_test.loc[i,'anomaly_candidates'] = 1

# df_sys_test['Label'] = df_sys_test.apply(anomaly_candidates_sys,axis=1)



df_test_2 = df_sys_test[['timestamp','Label']]
df_test_2.columns = ['TimeSlice','Label']
df_test_2['LogName'] = 'sysmonitor'
df_test_2 = df_test_2[['LogName', 'TimeSlice', 'Label']]



df_result = pd.concat([df_test_1,df_test_2])
df_result_new = df_result.drop_duplicates()
df_result_new = df_result_new.groupby(['LogName', 'TimeSlice']).sum()
df_result_new.reset_index(inplace=True)
df_result_new.to_csv("predict.csv",index=None)
df_result_new.to_csv(os.path.join(result_path, "predict.csv"),index=None)
# df_result.to_csv(os.path.join(result_path, "predict_finish.csv"),index=None)
pd.DataFrame(['TRUE'],columns=['result']).to_csv(os.path.join(result_path, "predict_finish.csv"),index=None)
# print(df_result.head())
print(df_result_new.shape)

