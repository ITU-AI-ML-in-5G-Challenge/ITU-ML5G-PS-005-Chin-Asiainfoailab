#!/bin/bash

workdir=$(cd $(dirname $0); pwd)
cd $workdir

appName='LOG-MATCH-MULTI'
app='log_match_multi_models'

id=`jps -m |grep ${app}|grep -v 'grep'|awk '{print $1}'`
if [ ! -n "$id" ]
then
        echo ${app} 'NOT START'
else
        echo "kill PID ${id}"
        kill -9 $id
        sleep 1s
fi


#yarn任务
appid=`yarn application -list|grep ${appName} |grep -v grep|awk '{ print $1}'`

if [ ! -n "$appid" ]
then
        echo "NO APPLICATION FOUND"
else
        echo ${appid}
        `yarn application -kill ${appid}`
fi
echo 'DONE'

nohup /usr/hdp/2.6.3.0-235/spark2/bin/spark-submit\
 --master yarn\
 --name ${appName}\
 --archives log_conda.tar.gz#log_conda \
 --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./log_conda/bin/python3 \
 --conf spark.pyspark.driver.python=${workdir}/log_conda/bin/python3 \
 --conf spark.pyspark.python=./log_conda/bin/python3 \
 --py-files ${workdir}/ftp_tool.py \
 --driver-memory 1g \
 --executor-memory 1g \
 --num-executors 2 \
 --executor-cores 2\
 --jars ${workdir}/spark-streaming-kafka-0-8-assembly_2.11-2.3.4.jar\
 ${workdir}/log_match_multi_models.py\
 ${1}\
 ${2}\
 10.19.90.9:6667\
 ${3}\
 aiops_log_mutil_topic > ${workdir}/logs/multi.log &

# 参数说明：
# 1. 包含多个形如“logType&logObject”的模型文件夹的目录
# 2. ftp信息
# 3. kafka broker list
# 4. 输入数据topic
# 5. 输出数据topic
