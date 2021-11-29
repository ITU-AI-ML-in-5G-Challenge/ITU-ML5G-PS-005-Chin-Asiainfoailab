#!/bin/bash

workdir=$(cd $(dirname $0); pwd)
cd $workdir

appName='LOG-MATCH-MULTI'
app='log_match_multi_models.py'

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
