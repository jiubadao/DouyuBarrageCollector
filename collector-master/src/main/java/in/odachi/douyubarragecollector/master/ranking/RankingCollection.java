package in.odachi.douyubarragecollector.master.ranking;

import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.constant.RedisKeys;
import in.odachi.douyubarragecollector.master.client.LocalCache;
import in.odachi.douyubarragecollector.util.FormatterUtil;
import in.odachi.douyubarragecollector.util.RedisUtil;
import org.apache.log4j.Logger;
import org.redisson.api.RScoredSortedSet;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public enum RankingCollection {
    // 单例模式
    INSTANCE;

    private static final Logger logger = Logger.getLogger(RankingCollection.class);

    private DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(Constants.DATE_PATTERN);

    private Map<Integer, Double> msgCountMap = new ConcurrentHashMap<>();

    private Map<Integer, Set<Integer>> msgUserMap = new ConcurrentHashMap<>();

    private Map<Integer, Double> giftCountMap = new ConcurrentHashMap<>();

    private Map<Integer, Set<Integer>> giftUserMap = new ConcurrentHashMap<>();

    private Map<Integer, Map<Integer, Integer>> giftTypeMap = new ConcurrentHashMap<>();

    // 5分钟排行榜：弹幕数量
    private RankingSlots msgCountSlots5 = new RankingSlots(5);

    // 60分钟排行榜：弹幕数量
    private RankingSlots msgCountSlots60 = new RankingSlots(60);

    // 5分钟排行榜：弹幕人次
    private RankingSlots msgUserSlots5 = new RankingSlots(5);

    // 60分钟排行榜：弹幕人次
    private RankingSlots msgUserSlots60 = new RankingSlots(60);

    // 5分钟排行榜：礼物数量
    private RankingSlots giftCountSlots5 = new RankingSlots(5);

    // 60分钟排行榜：礼物数量
    private RankingSlots giftCountSlots60 = new RankingSlots(60);

    // 5分钟排行榜：礼物人次
    private RankingSlots giftUserSlots5 = new RankingSlots(5);

    // 60分钟排行榜：礼物人次
    private RankingSlots giftUserSlots60 = new RankingSlots(60);

    // 5分钟排行榜：礼物价值
    private RankingSlots giftPriceSlots5 = new RankingSlots(5);

    // 60分钟排行榜：礼物价值
    private RankingSlots giftPriceSlots60 = new RankingSlots(60);

    /**
     * 新增一个消息数量
     */
    public void incrementMsgCount(Integer roomId) {
        msgCountMap.compute(roomId, (k, v) -> v == null ? 1 : v + 1);
    }

    /**
     * 新增一个消息数量
     */
    public void incrementMsgUser(Integer roomId, Integer userId) {
        incrementMsgUser(msgUserMap, roomId, userId);
    }

    private void incrementMsgUser(Map<Integer, Set<Integer>> userMap, Integer roomId, Integer userId) {
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

    /**
     * 新增一个礼物数量
     */
    public void incrementGiftCount(Integer roomId) {
        giftCountMap.compute(roomId, (k, v) -> v == null ? 1 : v + 1);
    }

    /**
     * 新增一个礼物数量
     */
    public void incrementGiftUser(Integer roomId, Integer userId) {
        incrementMsgUser(giftUserMap, roomId, userId);
    }

    /**
     * 新增一个礼物数量
     */
    public void incrementGiftType(Integer roomId, Integer giftId) {
        giftTypeMap.compute(roomId, (k, v) -> {
            if (v == null) {
                Map<Integer, Integer> types = new HashMap<>();
                types.compute(giftId, (k0, v0) -> v0 == null ? 1 : v0 + 1);
                return types;
            } else {
                v.compute(giftId, (k0, v0) -> v0 == null ? 1 : v0 + 1);
                return v;
            }
        });
    }

    /**
     * 统计某个时间段的消息数量
     */
    public void computeAndClearMsgCount() {
        Map<Integer, Double> resultMap = new HashMap<>();
        msgCountMap.keySet().forEach(roomId -> resultMap.put(roomId, msgCountMap.replace(roomId, 0d)));
        msgCountSlots5.putIntoSlots(resultMap);
        msgCountSlots60.putIntoSlots(resultMap);
        cacheToRedis(RedisKeys.DOUYU_RANK_ANCHOR_CHATMSG_COUNT_MINUTE_5, msgCountSlots5);
        cacheToRedis(RedisKeys.DOUYU_RANK_ANCHOR_CHATMSG_COUNT_MINUTE_60, msgCountSlots60);
        cacheToRedisDaily(resultMap, RedisKeys.DOUYU_RANK_ANCHOR_MSG_COUNT_PREFIX, RedisKeys.SUB_MSG_COUNT);
    }

    /**
     * 统计某个时间段的消息人次
     */
    public void computeAndClearMsgUser() {
        Map<Integer, Double> resultMap = new HashMap<>();
        msgUserMap.keySet().forEach(roomId -> resultMap.put(roomId, (double) msgUserMap.replace(roomId, new HashSet<>()).size()));
        msgUserSlots5.putIntoSlots(resultMap);
        msgUserSlots60.putIntoSlots(resultMap);
        cacheToRedis(RedisKeys.DOUYU_RANK_ANCHOR_CHATMSG_USER_MINUTE_5, msgUserSlots5);
        cacheToRedis(RedisKeys.DOUYU_RANK_ANCHOR_CHATMSG_USER_MINUTE_60, msgUserSlots60);
        cacheToRedisDaily(resultMap, RedisKeys.DOUYU_RANK_ANCHOR_MSG_USER_PREFIX, RedisKeys.SUB_MSG_USER);
    }

    /**
     * 统计某个时间段的礼物数量
     */
    public void computeAndClearGiftCount() {
        Map<Integer, Double> resultMap = new HashMap<>();
        giftCountMap.keySet().forEach(roomId -> resultMap.put(roomId, giftCountMap.replace(roomId, 0d)));
        giftCountSlots5.putIntoSlots(resultMap);
        giftCountSlots60.putIntoSlots(resultMap);
        cacheToRedis(RedisKeys.DOUYU_RANK_ANCHOR_DGB_COUNT_MINUTE_5, giftCountSlots5);
        cacheToRedis(RedisKeys.DOUYU_RANK_ANCHOR_DGB_COUNT_MINUTE_60, giftCountSlots60);
        cacheToRedisDaily(resultMap, RedisKeys.DOUYU_RANK_ANCHOR_DGB_COUNT_PREFIX, RedisKeys.SUB_DGB_COUNT);
    }

    /**
     * 统计某个时间段的礼物人次
     */
    public void computeAndClearGiftUser() {
        Map<Integer, Double> resultMap = new HashMap<>();
        giftUserMap.keySet().forEach(roomId -> resultMap.put(roomId, (double) giftUserMap.replace(roomId, new HashSet<>()).size()));
        giftUserSlots5.putIntoSlots(resultMap);
        giftUserSlots60.putIntoSlots(resultMap);
        cacheToRedis(RedisKeys.DOUYU_RANK_ANCHOR_DGB_USER_MINUTE_5, giftUserSlots5);
        cacheToRedis(RedisKeys.DOUYU_RANK_ANCHOR_DGB_USER_MINUTE_60, giftUserSlots60);
        cacheToRedisDaily(resultMap, RedisKeys.DOUYU_RANK_ANCHOR_DGB_USER_PREFIX, RedisKeys.SUB_DGB_USER);
    }

    /**
     * 统计某个时间段的礼物价值
     */
    public void computeAndClearGiftPrice() {
        Map<Integer, Double> giftPriceMap = new HashMap<>();
        giftTypeMap.keySet().forEach(roomId -> {
            Map<Integer, Integer> typeMap = giftTypeMap.replace(roomId, new ConcurrentHashMap<>());
            typeMap.forEach((giftId, count) -> {
                Map<String, Object> giftMap = LocalCache.INSTANCE.getGift(giftId);
                if (giftMap.size() <= 0) {
                    // 如果礼物未发现，则先尝试获取礼物
                    LocalCache.INSTANCE.queryRoomJson(roomId);
                    giftMap = LocalCache.INSTANCE.getGift(giftId);
                }
                if (giftMap.size() > 0) {
                    Integer type = FormatterUtil.parseInt(giftMap.get("type"));
                    Double price = FormatterUtil.parseInt(giftMap.get("pc")) * ((type == 2) ? 1 : 0.001);
                    giftPriceMap.compute(roomId, (k, v) -> v == null ? price : v + price);
                } else {
                    logger.error("Gift NOT found, trying query from room json: " + giftId + ", roomId: " + roomId);
                }
            });
        });
        giftPriceSlots5.putIntoSlots(giftPriceMap);
        giftPriceSlots60.putIntoSlots(giftPriceMap);
        cacheToRedis(RedisKeys.DOUYU_RANK_ANCHOR_DGB_PRICE_MINUTE_5, giftPriceSlots5);
        cacheToRedis(RedisKeys.DOUYU_RANK_ANCHOR_DGB_PRICE_MINUTE_60, giftPriceSlots60);
        cacheToRedisDaily(giftPriceMap, RedisKeys.DOUYU_RANK_ANCHOR_DGB_PRICE_PREFIX, RedisKeys.SUB_DGB_PRICE);
    }

    /**
     * 缓存结果到NoSQL数据库
     */
    private void cacheToRedis(String key, RankingSlots rankingSlots) {
        RScoredSortedSet<Integer> scoredSortedSet = RedisUtil.client.getScoredSortedSet(key);
        scoredSortedSet.clear();
        rankingSlots.querySlotSum().forEach((k, v) -> scoredSortedSet.add(v, k));
    }

    /**
     * 另存为历史周期数据
     */
    private void cacheToRedisDaily(Map<Integer, Double> dataToday, String rankKeyPrefix, String hashKey) {
        String today = LocalDate.now().format(dateFormatter);
        String rankKey = rankKeyPrefix + today;
        RScoredSortedSet<Integer> scoredSortedSet = RedisUtil.client.getScoredSortedSet(rankKey);
        scoredSortedSet.clear();
        dataToday.keySet().forEach(roomId -> {
            String detailKey = RedisKeys.DOUYU_DETAIL_ANCHOR_PREFIX + roomId + ":" + today;
            Map<String, Double> detailMap = RedisUtil.client.getMap(detailKey);
            Double score = detailMap.compute(hashKey, (k, v) -> v == null ? dataToday.get(roomId) : v + dataToday.get(roomId));
            scoredSortedSet.add(score, roomId);
        });
    }
}
