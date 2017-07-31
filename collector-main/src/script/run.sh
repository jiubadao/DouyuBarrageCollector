#!/usr/bin/env bash

mkdir -p logs
mkdir -p out
JVM_OPTS='-XX:+TieredCompilation -Xmx1024m -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:logs/gc.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=1 -XX:GCLogFileSize=100M'

CLASS_PATH="collector-main-2.0.jar":./conf:./lib/*

case $1 in
start)
    old=`ps -ef | grep 'in.odachi.douyubarragecollector.main.Main' | grep -v 'grep'  | awk -F" " '{print $2}'`
    if [ "" != "$old" ]; then
        echo old process is running
    else
        echo java ${JVM_OPTS} -cp ${CLASS_PATH} in.odachi.douyubarragecollector.main.Main
        java ${JVM_OPTS} -cp ${CLASS_PATH} in.odachi.douyubarragecollector.main.Main &
        echo start
    fi
;;
start-new)
    java ${JVM_OPTS} -cp ${CLASS_PATH} in.odachi.douyubarragecollector.main.Main &
    echo start-new
;;
stop)
    ps -ef | grep 'in.odachi.douyubarragecollector.main.Main' | grep -v 'grep'  | awk -F" " '{print $2}' | xargs -IA kill A
    echo stop
;;
restart)
    ps -ef | grep 'in.odachi.douyubarragecollector.main.Main' | grep -v 'grep'  | awk -F" " '{print $2}' | xargs -IA kill A
    echo java ${JVM_OPTS} -cp ${CLASS_PATH} -jar in.odachi.douyubarragecollector.main.Main
    java ${JVM_OPTS} -cp ${CLASS_PATH} in.odachi.douyubarragecollector.main.Main &
    echo restart
;;
status)
    old=`ps -ef | grep 'in.odachi.douyubarragecollector.main.Main' | grep -v 'grep'  | awk -F" " '{print $2}'`
    if [ "" != "${old}" ]; then
        echo process is running pid is ${old}
    else
        echo process is stop
    fi
;;
debug)
    old=`ps -ef | grep 'in.odachi.douyubarragecollector.main.Main' | grep -v 'grep'  | awk -F" " '{print $2}'`
    if [ "" != "${old}" ]; then
        echo old process is running
    else
        echo java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,address="9988",suspend=n ${JVM_OPTS} -cp ${CLASS_PATH} in.odachi.douyubarragecollector.main.Main
	java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,address="9988",suspend=n ${JVM_OPTS} -cp ${CLASS_PATH} in.odachi.douyubarragecollector.main.Main &
        echo debug
    fi
;;
*)
    echo need paras: start start-new stop restart status
;;
esac
