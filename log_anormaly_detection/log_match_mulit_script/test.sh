workdir=$(cd $(dirname $0); pwd)
cd $workdir

/usr/hdp/2.6.3.0-235/spark2/bin/spark-submit\
 --master yarn\
 --archives log_conda.tar.gz#log_conda \
 --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./log_conda/bin/python3 \
 --conf spark.pyspark.driver.python=${workdir}/log_conda/bin/python3 \
 --conf spark.pyspark.python=./log_conda/bin/python3 \
 --py-files ${workdir}/ftp_tool.py \
 --driver-memory 1g \
 --executor-memory 1g \
 --num-executors 2 \
 --executor-cores 2 \
 --jars ${workdir}/spark-streaming-kafka-0-8-assembly_2.11-2.3.4.jar\
 ${workdir}/log_match_multi_models.py\
 /data/test/output/logAnomalyDetect/pkl \
 $'{"host":"10.15.49.41","password":"ftP!123","port":22021,"username":"ftpUser"}' \
 10.19.90.9:6667\
 topic_log\
 aiops_log_mutil_topic
