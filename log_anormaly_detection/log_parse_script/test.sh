/usr/hdp/2.6.3.0-235/spark2/bin/spark-submit \
 --master yarn\
 --archives log_conda.tar.gz#log_conda \
 --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./log_conda/bin/python3 \
 --conf spark.pyspark.driver.python=/mnt/data/AIOps/logAnormalyDetection/ai-log-parse/log_conda/bin/python3 \
 --conf spark.pyspark.python=./log_conda/bin/python3 \
 --py-files /mnt/data/AIOps/logAnormalyDetection/ai-log-parse/ftp_tool.py\
 --executor-cores 1\
 --num-executors 1\
 --executor-memory 512m\
 --driver-memory 512m\
 /mnt/data/AIOps/logAnormalyDetection/ai-log-parse/log_parse.py\
 ftp://ftpUser:ftP!123@10.15.49.41:22021/data/input/test/train_data.csv \
 /data/output/logAnomalyDetect/template \
 /data/output/logAnomalyDetect/pickle_files \
 'http://10.19.90.8:10069/api/logdetection/trainend' \
 '{"ai_task_id":"6f2a693b4057463aa855e67125762266","bracket_flag":"1","clustering_algorithm":"SPELL","date_flag":"1","ip_flag":"1","similarity_method":"LCS","similarity_threshold":"0.3","special_string_flag":"1","url_flag":"1"}' \
 '{"password":"ftP!123","port":"22021","host":"10.15.49.41","username":"ftpUser"}'