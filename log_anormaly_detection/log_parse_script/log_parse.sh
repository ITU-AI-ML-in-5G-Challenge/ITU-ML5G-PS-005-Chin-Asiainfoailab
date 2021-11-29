#!/bin/bash
workdir=$(cd $(dirname $0); pwd)
cd ${workdir}

echo ${1} ${2} ${3} ${4} ${5} ${6} >> ${workdir}/logs/path.log

nohup /usr/hdp/2.6.3.0-235/spark2/bin/spark-submit \
 --master yarn\
 --archives log_conda.tar.gz#log_conda \
 --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./log_conda/bin/python3 \
 --conf spark.pyspark.driver.python=${workdir}/log_conda/bin/python3 \
 --conf spark.pyspark.python=./log_conda/bin/python3 \
 --py-files ${workdir}/ftp_tool.py\
 --executor-cores 2\
 --num-executors 2\
 --executor-memory 1g\
 --driver-memory 512m\
 ${workdir}/log_parse.py\
 ${1}\
 ${2}\
 ${3}\
 ${4}\
 ${5}\
 ${6}\
  > ${workdir}/logs/parse.log &
 
 # 上面六个参数分别为：
 # 1. 输入数据的文件名
 # 2. 输出的可读模板文件名
 # 3. 输出不可读文件（pickle序列化文件）的文件夹
 # 4. url:http+post
 # 5. 输入的json格式的可变参数和taskid
 # 6. 训练过程的并发数，默认不填。为1时不连接spark，直接在本地单线程运行
