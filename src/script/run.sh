#!/bin/bash

mkdir -p logs
JVM_OPTS='-XX:+TieredCompilation -Xmx1024m -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:logs/gc.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=1 -XX:GCLogFileSize=100M'

CLASS_PATH="DouyuBarrageCollector-2.0.jar":./conf:./lib/*

case $1 in
start)
    old=`ps -ef | grep 'DouyuBarrageCollector-' | grep -v 'grep'  | awk -F" " '{print $2}'`
    if [ "" != "$old" ]; then
        echo old process is running
    else
        echo java ${JVM_OPTS} -cp ${CLASS_PATH} in.odachi.douyubarragecollector.app.Main
        java ${JVM_OPTS} -cp ${CLASS_PATH} in.odachi.douyubarragecollector.app.Main &
        echo start
    fi
;;
start-new)
    java ${JVM_OPTS} -cp ${CLASS_PATH} in.odachi.douyubarragecollector.app.Main &
    echo start-new
;;
stop)
    ps -ef | grep 'DouyuBarrageCollector-' | grep -v 'grep'  | awk -F" " '{print $2}' | xargs -IA kill A
    echo stop
;;
restart)
    ps -ef | grep 'DouyuBarrageCollector-' | grep -v 'grep'  | awk -F" " '{print $2}' | xargs -IA kill A
    echo java ${JVM_OPTS} -cp ${CLASS_PATH} -jar in.odachi.douyubarragecollector.app.Main
    java ${JVM_OPTS} -cp ${CLASS_PATH} in.odachi.douyubarragecollector.app.Main &
    echo restart
;;
status)
    old=`ps -ef | grep 'DouyuBarrageCollector-' | grep -v 'grep'  | awk -F" " '{print $2}'`
    if [ "" != "${old}" ]; then
        echo process is running pid is ${old}
    else
        echo process is stop
    fi
;;
debug)
    old=`ps -ef | grep 'DouyuBarrageCollector-' | grep -v 'grep'  | awk -F" " '{print $2}'`
    if [ "" != "${old}" ]; then
        echo old process is running
    else
        echo java -agentpath:/opt/jprofiler9/bin/linux-x64/libjprofilerti.so=port=8849 ${JVM_OPTS} -cp ${CLASS_PATH} in.odachi.douyubarragecollector.app.Main
        java -agentpath:/opt/jprofiler9/bin/linux-x64/libjprofilerti.so=port=8849 ${JVM_OPTS} -cp ${CLASS_PATH} in.odachi.douyubarragecollector.app.Main &
        echo debug
    fi
;;
*)
    echo need paras: start start-new stop restart status
;;
esac
