#!/usr/bin/env bash

mkdir -p logs
mkdir -p out
JVM_OPTS='-XX:+TieredCompilation -Xmx1024m -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:logs/master-gc.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=1 -XX:GCLogFileSize=100M'

CLASS_PATH=./conf:./lib/*

case $1 in
start)
    old=`ps -ef | grep 'in.odachi.douyubarragecollector.master.main.Master' | grep -v 'grep'  | awk -F" " '{print $2}'`
    if [ "" != "$old" ]; then
        echo old process is running
    else
        echo java ${JVM_OPTS} -cp ${CLASS_PATH} in.odachi.douyubarragecollector.master.main.Master
        java ${JVM_OPTS} -cp ${CLASS_PATH} in.odachi.douyubarragecollector.master.main.Master &
        echo start
    fi
;;
start-new)
    java ${JVM_OPTS} -cp ${CLASS_PATH} in.odachi.douyubarragecollector.master.main.Master &
    echo start-new
;;
stop)
    ps -ef | grep 'in.odachi.douyubarragecollector.master.main.Master' | grep -v 'grep'  | awk -F" " '{print $2}' | xargs -IA kill A
    echo stop
;;
restart)
    ps -ef | grep 'in.odachi.douyubarragecollector.master.main.Master' | grep -v 'grep'  | awk -F" " '{print $2}' | xargs -IA kill A
    echo java ${JVM_OPTS} -cp ${CLASS_PATH} -jar in.odachi.douyubarragecollector.master.main.Master
    java ${JVM_OPTS} -cp ${CLASS_PATH} in.odachi.douyubarragecollector.master.main.Master &
    echo restart
;;
status)
    old=`ps -ef | grep 'in.odachi.douyubarragecollector.master.main.Master' | grep -v 'grep'  | awk -F" " '{print $2}'`
    if [ "" != "${old}" ]; then
        echo process is running pid is ${old}
    else
        echo process is stop
    fi
;;
debug)
    old=`ps -ef | grep 'in.odachi.douyubarragecollector.master.main.Master' | grep -v 'grep'  | awk -F" " '{print $2}'`
    if [ "" != "${old}" ]; then
        echo old process is running
    else
        echo java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,address="9988",suspend=n ${JVM_OPTS} -cp ${CLASS_PATH} in.odachi.douyubarragecollector.master.main.Master
	java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,address="9988",suspend=n ${JVM_OPTS} -cp ${CLASS_PATH} in.odachi.douyubarragecollector.master.main.Master &
        echo debug
    fi
;;
*)
    echo need paras: start start-new stop restart status
;;
esac
