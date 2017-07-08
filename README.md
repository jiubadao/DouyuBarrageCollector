# 斗鱼弹幕采集工具

## 可以做什么

DouyuBarrageCollector是一个采集斗鱼直播平台弹幕信息的小工具，基于官方发布的“[斗鱼弹幕服务器第三方接入协议v1.4.1](http://dev-bbs.douyutv.com/forum.php?mod=attachment&aid=MjYxfDFiMzgyZTU1fDE0OTE3MTQ2MDl8MHwxMTU%3D)”制作而成。
本工具仅供学习和研究使用，通过该工具采集的所有信息版权均归斗鱼所有，特此声明。

**Copyright (C) 武汉斗鱼网络科技有限公司（2016）. All Rights Reserved.**

截止目前，**该程序可以稳定运行超过xx天**。

## 如何运行

注意按注释要求修改配置文件[`barrage.properties`](https://github.com/zhaopeizhi/DouyuBarrageCollector/blob/master/src/main/resources/barrage.properties)，然后在应用根目录下使用mvn编译出包：`mvn package`。

输出目录结构：

    \---target
        |   DouyuBarrageCollector-2.0.jar
        |   run.sh
        |
        +---conf
        |       barrage.properties
        |       quartz.properties
        |       log4j.properties
        |
        +---lib
        |       ......
        |
        \---out
                exp.sh

启动程序（推荐使用2G及以上内存运行程序。）：

    cd target
    sh run.sh start

## 弹幕分析

对采集到的数据做一些分析和可视化工作：

[斗鱼分析/DOUYU Analytic](http://odachi.in:8080)

## 其他

如果有什么问题或者建议都可以在[Issue](https://github.com/zhaopeizhi/DouyuBarrageCollector/issues)和我讨论。
