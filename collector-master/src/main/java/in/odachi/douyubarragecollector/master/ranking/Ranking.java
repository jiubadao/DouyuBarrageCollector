package in.odachi.douyubarragecollector.master.ranking;

import in.odachi.douyubarragecollector.master.client.LocalCache;
import in.odachi.douyubarragecollector.master.util.MasterUtil;
import in.odachi.douyubarragecollector.util.FormatterUtil;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 实时排名
 */
public class Ranking {

    private static final Logger logger = Logger.getLogger(Ranking.class);

    /**
     * 增加一个弹幕计数
     */
    public static void incrementMessage(Integer roomId, Integer userId) {
        // 弹幕数量
        RankingCollection.msgCountMap.compute(roomId, (k, v) -> v == null ? 1 : v + 1);
        // 弹幕人次
        incrementUserCount(RankingCollection.msgUserMap, roomId, userId);
    }

    /**
     * 增加一个礼物计数
     */
    public static void incrementGift(Integer roomId, Integer userId, Integer gId) {
        // 礼物数量
        RankingCollection.giftCountMap.compute(roomId, (k, v) -> v == null ? 1 : v + 1);
        // 礼物人次
        incrementUserCount(RankingCollection.giftUserMap, roomId, userId);
        // 礼物价值
        Map<String, Object> giftMap = LocalCache.INSTANCE.getGift(gId);
        if (giftMap.size() > 0) {
            Integer type = FormatterUtil.parseInt(giftMap.get("type"));
            double rate = (type == 2) ? 1 : 0.001;
            Double price = FormatterUtil.parseInt(giftMap.get("pc")) * rate;
            RankingCollection.giftPriceMap.compute(roomId, (k, v) -> v == null ? price : v + price);
        } else {
            logger.error("Gift NOT found, trying query from json: " + gId + ", roomId: " + roomId);
            LocalCache.INSTANCE.queryRoomJson(roomId);
        }
    }

    private static void incrementUserCount(Map<Integer, Set<Integer>> userMap, Integer roomId, Integer userId) {
        userMap.compute(roomId, (k, v) -> {
            if (v == null) {
                Set<Integer> userSet = new HashSet<>();
                userSet.add(userId);
                return userSet;
            } else {
                v.add(userId);
                return v;
            }
        });
    }
}
