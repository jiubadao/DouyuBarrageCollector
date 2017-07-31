package in.odachi.douyubarragecollector.master.ranking;

import in.odachi.douyubarragecollector.constant.RedisKeys;
import in.odachi.douyubarragecollector.util.RedisUtil;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RankingCollection {

    public static Map<Integer, Double> msgCountMap = new ConcurrentHashMap<>();

    public static Map<Integer, Set<Integer>> msgUserMap = new ConcurrentHashMap<>();

    public static Map<Integer, Double> giftCountMap = new ConcurrentHashMap<>();

    public static Map<Integer, Set<Integer>> giftUserMap = new ConcurrentHashMap<>();

    public static Map<Integer, Double> giftPriceMap = new ConcurrentHashMap<>();

    // 5分钟排行榜：弹幕数量
    public static RankingSlots msgCountSlots5 = new RankingSlots(5);

    // 60分钟排行榜：弹幕数量
    public static RankingSlots msgCountSlots60 = new RankingSlots(60);

    // 5分钟排行榜：弹幕人次
    public static RankingSlots msgUserSlots5 = new RankingSlots(5);

    // 60分钟排行榜：弹幕人次
    public static RankingSlots msgUserSlots60 = new RankingSlots(60);

    // 5分钟排行榜：礼物数量
    public static RankingSlots giftCountSlots5 = new RankingSlots(5);

    // 60分钟排行榜：礼物数量
    public static RankingSlots giftCountSlots60 = new RankingSlots(60);

    // 5分钟排行榜：礼物人次
    public static RankingSlots giftUserSlots5 = new RankingSlots(5);

    // 60分钟排行榜：礼物人次
    public static RankingSlots giftUserSlots60 = new RankingSlots(60);

    // 5分钟排行榜：礼物价值
    public static RankingSlots giftPriceSlots5 = new RankingSlots(5);

    // 60分钟排行榜：礼物价值
    public static RankingSlots giftPriceSlots60 = new RankingSlots(60);

    public static Map<Integer, Double> msgCountToday = RedisUtil.client.getMap(
            RedisKeys.DOUYU_RANK_ANCHOR_CHATMSG_COUNT_TODAY_SLOT);

    public static Map<Integer, Double> msgUserToday = RedisUtil.client.getMap(
            RedisKeys.DOUYU_RANK_ANCHOR_CHATMSG_USER_TODAY_SLOT);

    public static Map<Integer, Double> giftCountToday = RedisUtil.client.getMap(
            RedisKeys.DOUYU_RANK_ANCHOR_DGB_COUNT_TODAY_SLOT);

    public static Map<Integer, Double> giftUserToday = RedisUtil.client.getMap(
            RedisKeys.DOUYU_RANK_ANCHOR_DGB_USER_TODAY_SLOT);

    public static Map<Integer, Double> giftPriceToday = RedisUtil.client.getMap(
            RedisKeys.DOUYU_RANK_ANCHOR_DGB_PRICE_TODAY_SLOT);
}
