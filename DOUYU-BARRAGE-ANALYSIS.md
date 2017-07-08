# 斗鱼主播/弹幕分析

## 房间数据分析

- 系统抓取了3个整天全部的房间和弹幕信息，累计共8W多个房间有上线记录。

- 抓取的数据起始时间和结束时间：
    
        sqlite> SELECT min(t.date_time), max(t.date_time) FROM room t;
        min(t.date_time)|max(t.date_time)
        2017-04-28 00:05:25|2017-04-30 23:55:13
    
- 其中关注数大于20万的房间数（后续仅分析这309个房间的数据）：

        sqlite> SELECT count(DISTINCT(t.room_id)) FROM room t;
        count(DISTINCT(t.room_id))  
        309

- 按分类汇总房间数：
    
        sqlite> SELECT t.cate_name, count(t.cate_name) as cate_count 
        FROM room t GROUP BY t.cate_name ORDER BY cate_count DESC;
        cate_name|cate_count
        视听点评|15577
        英雄联盟|12665
        户外|4691
        主机游戏|3994
        二次元|2605
        DOTA2|1855
        王者荣耀|1844
        炉石传说|1531
        颜值|1446
        美食|1302
        守望先锋|1117
        穿越火线|826
        DNF|763
        汽车|749
        时尚|690
        怀旧游戏|595
        环球鱼乐|484
        魔兽争霸|418
        棋牌娱乐|371
        足球竞技|356
        星秀|299
        梦幻西游手游|263
        音乐|216
        竞速游戏|206
        火影忍者|141
        格斗游戏|100
        科普|99
        数码科技|97
        鱼教|93
        至尊嗨场|87
        新游中心|72
        天天狼人杀|28
        天涯明月刀|22
        企鹅直播|19
        综合手游|13
        球球大作战|10
        CS:GO|2
        生活秀|2
        绝地求生|1
        
- 每个分类下关注数最多的主播：
        
        sqlite> SELECT t.cate_name, t.owner_name, max(CAST(t.fans_num as INTEGER)) as fans_num 
        FROM room t GROUP BY t.cate_name ORDER BY fans_num DESC;
        cate_name|owner_name|fans_num
        英雄联盟|White55开解说|8163700
        炉石传说|陈一发儿|3693142
        主机游戏|陈一发儿|3691551
        时尚|guoyun丶mini|2557423
        户外|高冷男神钱小佳|2029376
        颜值|软妹小九九丶|1915766
        视听点评|狂拽酷炫|1420301
        王者荣耀|KPL斗鱼官方直播间1|1317621
        二次元|轩子巨2兔|1305062
        天天狼人杀|阿科哥|1188437
        科普|火星户外|1119526
        CS:GO|火星户外|1119519
        美食|大胃王密子君|1082991
        DOTA2|yyfyyf|969661
        环球鱼乐|日本超模武田华恋|914958
        守望先锋|夏一可死毒舌_|880546
        新游中心|恶魔qq|848277
        音乐|欢欢女神|761812
        至尊嗨场|主播阿郎|744804
        棋牌娱乐|DG女团丶闪现君|670975
        星秀|小学生赛高呀|660066
        DNF|胜哥002|645996
        汽车|主播AJ|623687
        魔兽争霸|澄海TMac|597877
        足球竞技|老佳|527011
        企鹅直播|老佳|526546
        穿越火线|SherlcokHolmes|458695
        格斗游戏|战神_河池VR|408289
        绝地求生|Jackman|343192
        数码科技|科技美学中国|321295
        天涯明月刀|Reena嘉一|320905
        火影忍者|ice秋风|295188
        鱼教|上虞娜娜|285497
        怀旧游戏|宋先生|262163
        竞速游戏|DouyuTV华建|232542
        球球大作战|麻瓜苏|231939
        梦幻西游手游|小儿郎丶丶|231300
        综合手游|麻瓜苏|230055
        生活秀|BabyMik|207823
        
- 全部分类下关注数TOP20的主播：
        
        sqlite> SELECT t.cate_name, t.owner_name, t.room_id, max(CAST(t.fans_num as INTEGER)) as fans_num 
        FROM room t GROUP BY t.room_id ORDER BY fans_num DESC LIMIT 20;
        cate_name|owner_name|room_id|fans_num
        英雄联盟|White55开解说|138286|8163700
        英雄联盟|冯提莫|71017|5726669
        英雄联盟|芜湖大司马丶|606118|4570685
        英雄联盟|老实敦厚的笑笑|154537|4552330
        炉石传说|陈一发儿|67373|3693142
        炉石传说|七哥张琪格|65251|3252211
        英雄联盟|主播油条|56040|3161829
        英雄联盟|饼干狂魔MasterB|4809|3133832
        时尚|guoyun丶mini|10903|2557423
        英雄联盟|萌面酥|266055|2302978
        英雄联盟|微笑|16101|2253048
        英雄联盟|洞主丨歌神洞庭湖|138243|2118823
        英雄联盟|黑白锐雯|93912|2077575
        户外|高冷男神钱小佳|229346|2029376
        英雄联盟|东北大鹌鹑|96291|1920454
        英雄联盟|英雄联盟官方赛事|288016|1917984
        颜值|软妹小九九丶|265688|1915766
        户外|霹雳爷们儿|274874|1908133
        英雄联盟|我叫撸管飞|453751|1789925
        英雄联盟|霸哥来啦|321358|1432351
        
- 全部分类下平均人气TOP20的主播： 
        
        sqlite> SELECT t.cate_name, t.owner_name, avg(t.online) as avg_online 
        FROM room t GROUP BY t.room_id ORDER BY avg_online DESC LIMIT 20;
        cate_name|owner_name|avg_online
        英雄联盟|White55开解说|1069122.59090909
        英雄联盟|老实敦厚的笑笑|943146.920863309
        英雄联盟|英雄联盟官方赛事二|908810.006968641
        DOTA2|yyfyyf|691119.05050505
        英雄联盟|冯提莫|556951.384615385
        英雄联盟|英雄联盟官方赛事|538431.268365817
        英雄联盟|芜湖大司马丶|534612.764705882
        音乐|暗杠小发|509386.052631579
        英雄联盟|洞主丨歌神洞庭湖|497176.489795918
        英雄联盟|SKT直播faker|452971.324324324
        英雄联盟|东北大鹌鹑|399517.947826087
        王者荣耀|小QIMBA|395791.068181818
        炉石传说|陈一发儿|377645.527272727
        英雄联盟|主播油条|371235.755244755
        英雄联盟|微笑|368801.705882353
        二次元|轩子巨2兔|353184.225806452
        DOTA2|820邹倚天|321481.154811715
        王者荣耀|小小青蛙笑i|308953.336206897
        英雄联盟|霸哥来啦|297793.084210526
        英雄联盟|饼干狂魔MasterB|289774.956989247
        
- 全部分类下平均人气/关注数TOP20的主播：
        
        sqlite> SELECT t0.*, round(t0.avg_online/t0.fans_num,2) as rate FROM 
        (SELECT t.cate_name, t.owner_name, max(t.fans_num) as fans_num, avg(t.online) as avg_online 
        FROM room t GROUP BY t.room_id ) t0 ORDER BY rate DESC LIMIT 20;
        cate_name|owner_name|fans_num|avg_online|rate
        音乐|暗杠小发|282803|509386.052631579|1.8
        英雄联盟|英雄联盟官方赛事二|645409|908810.006968641|1.41
        DOTA2|820邹倚天|344459|321481.154811715|0.93
        王者荣耀|小QIMBA|464936|395791.068181818|0.85
        英雄联盟|SKT直播peanut|220836|156184.505494505|0.71
        王者荣耀|小小青蛙笑i|433497|308953.336206897|0.71
        DOTA2|yyfyyf|969661|691119.05050505|0.71
        王者荣耀|声优夏天|240648|163267.802816901|0.68
        音乐|卖血哥|263913|168546.566037736|0.64
        英雄联盟|SKT直播faker|759558|452971.324324324|0.6
        王者荣耀|荣耀英雄宋人头|322475|187600.423076923|0.58
        火影忍者|ice秋风|295188|162414.411347518|0.55
        主机游戏|热游联盟狐狸|323316|152425.5625|0.47
        炉石传说|可爱的小弱鸡|256516|119006.31092437|0.46
        颜值|麻瓜苏|232308|101672.917241379|0.44
        DOTA2|单车比DC讲道理|254807|109086.58313253|0.43
        魔兽争霸|ForeverTED|232586|97910.1968503937|0.42
        DOTA2|张宁_xiao8|497322|201733.051724138|0.41
        炉石传说|炉石丶春哥|290388|109478.17218543|0.38
        炉石传说|炉石丶啦啦啦|605305|204014.683453237|0.34
        
- 全部分类下最高人气TOP20的主播：
        
        sqlite> SELECT t.cate_name, t.owner_name, max(CAST(t.fans_num as INTEGER)) as fans_num, 
        max(CAST(t.online as INTEGER)) as max_online 
        FROM room t GROUP BY t.room_id ORDER BY max_online DESC LIMIT 20;
        cate_name|owner_name|fans_num|max_online
        英雄联盟|英雄联盟官方赛事|1917984|5673046
        英雄联盟|英雄联盟官方赛事二|645409|5079409
        英雄联盟|老实敦厚的笑笑|4552330|1702035
        DOTA2|yyfyyf|969661|1568213
        英雄联盟|White55开解说|8163700|1428895
        DOTA2|820邹倚天|344459|1105781
        王者荣耀|KPL斗鱼官方直播间1|1317621|1077042
        英雄联盟|洞主丨歌神洞庭湖|2118823|926877
        英雄联盟|SKT直播faker|759558|904969
        英雄联盟|东北大鹌鹑|1920454|738838
        王者荣耀|小QIMBA|464936|715284
        英雄联盟|饼干狂魔MasterB|3133832|709007
        音乐|暗杠小发|282803|706817
        英雄联盟|芜湖大司马丶|4570685|696426
        英雄联盟|冯提莫|5726669|667464
        主机游戏|陈一发儿|3693142|585691
        英雄联盟|主播油条|3161829|577910
        王者荣耀|小小青蛙笑i|433497|567250
        炉石传说|暴雪游戏频道|596115|565569
        英雄联盟|微笑|2253048|553853
        
- 全部分类按小时汇总平均在线人气：
        
        sqlite> SELECT 'H'||substr(t.date_time,12,2) as hour, round(avg(t.online), 0) as avg_online 
        FROM room t GROUP BY substr(t.date_time,12,2);
        hour|avg_online
        H00|76930.0
        H01|63996.0
        H02|50851.0
        H03|45422.0
        H04|38289.0
        H05|28177.0
        H06|21433.0
        H07|19943.0
        H08|26586.0
        H09|43389.0
        H10|48764.0
        H11|52744.0
        H12|61206.0
        H13|61681.0
        H14|52732.0
        H15|62891.0
        H16|87412.0
        H17|104939.0
        H18|104381.0
        H19|99280.0
        H20|78374.0
        H21|77288.0
        H22|79662.0
        H23|81801.0
            

## 弹幕数据分析
    
- 按日期分组汇总弹幕数量：

        sqlite> SELECT substr(date_time,0,11) as date, COUNT(0) FROM chatmsg GROUP BY date;
        date|COUNT(0)
        2017-04-28|4503536
        2017-04-29|4842585
        2017-04-30|4872683
        
- 发表过弹幕的用户总数：

        sqlite> SELECT count(DISTINCT(t.uid)) FROM chatmsg t;
        count(DISTINCT(t.uid))
        1734973

- 发表过弹幕的用户等级分布：

        sqlite> SELECT t.level, count(1) as count FROM chatmsg t GROUP BY t.level ORDER BY count DESC LIMIT 10;
        level|count
        15|848683
        14|822831
        13|781986
        16|779867
        12|703205
        1|696784
        17|695007
        5|657462
        7|644669
        6|641169

- 发表弹幕数TOP20用户榜单：

        sqlite> SELECT t.uid, count(1) as count FROM chatmsg t GROUP BY t.uid ORDER BY count DESC LIMIT 20;
        uid|count
        21360686|25735
        4654622|13151
        9415528|11250
        4264253|10355
        2473450|10086
        275736|9436
        168999|9271
        3715613|8558
        132750171|8151
        76211395|7647
        39404106|6805
        10216315|6380
        4150734|6273
        58838|5892
        4621564|5889
        535025|5649
        46311328|5115
        2261082|4510
        217139|4350
        132248146|4026

- 累计弹幕总次数排行：

        sqlite> SELECT r.cate_name, t.rid, r.owner_name, count(0) as count FROM chatmsg t 
        LEFT JOIN (SELECT * FROM room_all r0 GROUP BY r0.room_id) r on r.room_id = t.rid 
        GROUP BY t.rid ORDER BY count DESC LIMIT 20;
        cate_name|rid|owner_name|count
        英雄联盟|606118|芜湖大司马丶|494751
        户外|105025|赛文柒Seven|432990
        DOTA2|58428|yyfyyf|416658
        英雄联盟|6324|抽象工作室upupup|365844
        英雄联盟|138243|洞主丨歌神洞庭湖|351174
        DOTA2|64609|张宁_xiao8|316426
        户外|229346|高冷男神钱小佳|310485
        英雄联盟|288016|英雄联盟官方赛事|287550
        炉石传说|525207|暴雪游戏频道|237763
        炉石传说|67373|陈一发儿|211007
        二次元|1275878|暴走漫画|205357
        DOTA2|339715|单车比DC讲道理|200663
        英雄联盟|154537|老实敦厚的笑笑|195328
        DOTA2|507882|820邹倚天|194482
        英雄联盟|424559|英雄联盟官方赛事二|182205
        王者荣耀|573449|小小青蛙笑i|168980
        英雄联盟|453751|我叫撸管飞|164903
        视听点评|122402|进击の神乐|159937
        英雄联盟|13703|ShinyRuoの|143889
        户外|320155|主播阿郎|138151

- 累计弹幕人次排行：

        sqlite> SELECT r.cate_name, t.rid, r.owner_name, count(0) as count FROM chatmsg t 
        LEFT JOIN (SELECT * FROM room_all r0 GROUP BY r0.room_id) r on r.room_id = t.rid 
        GROUP BY t.rid, t.uid ORDER BY count DESC LIMIT 20;
        cate_name|rid|owner_name|count
        二次元|592360|sandy和mandy副房间备用|25735
        视听点评|122402|进击の神乐|13151
        视听点评|206858|所有人都在装|11242
        视听点评|218859|UnaDirezione|10354
        视听点评|248753|s1134|10086
        视听点评|36337|酷炫小剧场|9436
        视听点评|4332|狂拽酷炫|9267
        视听点评|85894|进击的神乐|8528
        英雄联盟|6324|抽象工作室upupup|8151
        英雄联盟|6324|抽象工作室upupup|6732
        英雄联盟|6324|抽象工作室upupup|6304
        视听点评|96577|UnaDireziome|6273
        视听点评|20415|Sai_C|5892
        视听点评|101217|孙小夕|5889
        英雄联盟|6324|抽象工作室upupup|5640
        英雄联盟|244036|大莉cc|5000
        视听点评|66786|人生若如初见21|4510
        视听点评|6540|酷酷炫炫|4350
        视听点评|252802|狂仔酷炫|4296
        二次元|1275878|暴走漫画|3992


## 礼物数据分析
    
- 按日期分组汇总礼物数量：

        sqlite> SELECT substr(date_time,0,11) as date, COUNT(0) FROM dgb GROUP BY date;
        date|COUNT(0)
        2017-04-28|5136785
        2017-04-29|4805045
        2017-04-30|5115166
    
- 赠送过礼物的用户总数：

        sqlite> SELECT count(DISTINCT(t.uid)) FROM dgb t;
        count(DISTINCT(t.uid))
        697956
        
- 赠送过礼物的用户等级分布：

        sqlite> SELECT t.level, count(1) as count FROM dgb t GROUP BY t.level ORDER BY count DESC LIMIT 10;
        level|count
        14|1319992
        13|1284672
        15|1252668
        12|1079552
        16|1077963
        11|898759
        17|856727
        10|802265
        8|728346
        9|726975

- 赠送礼物数TOP20用户榜单：

        sqlite> SELECT t.uid, count(1) as count FROM dgb t GROUP BY t.uid ORDER BY count DESC LIMIT 20;
        uid|count
        993996|13520
        3045278|11257
        2851067|10247
        124980609|10017
        72837659|3764
        4702493|2965
        66328765|2959
        21007419|2868
        59646355|2854
        58814696|2589
        2240755|2482
        109895891|2457
        109268412|1942
        25449529|1904
        5282838|1858
        101791004|1795
        14420353|1769
        17726538|1745
        27778294|1552
        42785524|1505

 - 累计礼物总次数排行：
 
        sqlite> SELECT r.cate_name, t.rid, r.owner_name, count(0) as count FROM dgb t 
        LEFT JOIN (SELECT * FROM room_all r0 GROUP BY r0.room_id) r on r.room_id = t.rid 
        GROUP BY t.rid ORDER BY count DESC LIMIT 20;
        cate_name|rid|owner_name|count
        英雄联盟|606118|芜湖大司马丶|668914
        DNF|28101|云彩上的翅膀|624355
        DOTA2|58428|yyfyyf|489671
        炉石传说|67373|陈一发儿|405554
        英雄联盟|271934|叫我久哥哥|284111
        户外|229346|高冷男神钱小佳|249881
        英雄联盟|96291|东北大鹌鹑|236488
        英雄联盟|6324|抽象工作室upupup|225276
        英雄联盟|154537|老实敦厚的笑笑|223993
        二次元|196|小缘|212843
        户外|105025|赛文柒Seven|211699
        户外|430489|长沙乡村敢死队|185830
        炉石传说|633019|炉石丶啦啦啦|185336
        英雄联盟|453751|我叫撸管飞|179598
        英雄联盟|56040|主播油条|176031
        守望先锋|24422|pigff|166924
        主机游戏|71415|寅子|163707
        英雄联盟|4809|饼干狂魔MasterB|147690
        户外|468241|魅力生活i|143015
        主机游戏|74751|超级小桀|141139
 
 - 累计礼物人次排行：
 
        sqlite> SELECT r.cate_name, t.rid, r.owner_name, count(0) as count FROM dgb t 
        LEFT JOIN (SELECT * FROM room_all r0 GROUP BY r0.room_id) r on r.room_id = t.rid 
        GROUP BY t.rid, t.uid ORDER BY count DESC LIMIT 20;
        cate_name|rid|owner_name|count
        户外|229346|高冷男神钱小佳|13490
        二次元|196|小缘|11257
        DOTA2|20360|Pc冷冷|10247
        户外|442836|Leona娜姐|10015
        户外|430489|长沙乡村敢死队|3759
        颜值|761727|灬娇妹儿|2949
        二次元|656971|简言啦噜噜噜|2868
        户外|430489|长沙乡村敢死队|2854
        时尚|638575|丽亚celiahunne|2783
        颜值|761727|灬娇妹儿|2589
        主机游戏|85963|主播温州炮哥|2338
        英雄联盟|288016|英雄联盟官方赛事|1942
        二次元|292081|在下萝莉控ii|1900
        二次元|196|小缘|1858
        英雄联盟|1691187|SKT直播peanut|1745
        户外|105025|赛文柒Seven|1668
        主机游戏|85963|主播温州炮哥|1521
        英雄联盟|329364|老阳解说|1454
        英雄联盟|12313|叶音符|1441
        颜值|654083|Hi丶兜仔|1391


## 关键词提取

- 全部弹幕关键词词云：

- 按词频：58428

![58428](https://github.com/zhaopeizhi/DouyuBarrageCollector/blob/master/image/chatmsg-txt-20170324-58428.csv.cut.png)

- 按词频（tf-idf）：58428

![58428](https://github.com/zhaopeizhi/DouyuBarrageCollector/blob/master/image/chatmsg-txt-20170324-58428.csv.extract.png)
