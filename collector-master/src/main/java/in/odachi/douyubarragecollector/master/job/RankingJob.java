package in.odachi.douyubarragecollector.master.job;

import com.jcabi.aspects.Loggable;
import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.constant.RedisKeys;
import in.odachi.douyubarragecollector.master.ranking.RankingCollection;
import in.odachi.douyubarragecollector.master.ranking.RankingSlots;
import in.odachi.douyubarragecollector.util.RedisUtil;
import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.redisson.api.RScoredSortedSet;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 实时排行任务
 */
@DisallowConcurrentExecution
public class RankingJob implements Job {

    private static final Logger logger = Logger.getLogger(TokenizerJob.class);

    private DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(Constants.DATE_PATTERN);

    @Override
    @Loggable
    public void execute(JobExecutionContext context) throws JobExecutionException {
        // 每天凌晨清空当天的统计数据，并将结果统计到历史数据中
        boolean morningMark = false;
        if (LocalDateTime.now().getHour() == 0 && LocalDateTime.now().getMinute() == 0) {
            morningMark = true;
        }

        // 实时统计弹幕数量
        Map<Integer, Double> msgCountMap = new HashMap<>(RankingCollection.msgCountMap);
        RankingCollection.msgCountMap.clear();
        RankingCollection.msgCountSlots5.putIntoSlots(msgCountMap);
        RankingCollection.msgCountSlots60.putIntoSlots(msgCountMap);
        msgCountMap.forEach((key, value) -> RankingCollection.msgCountToday.compute(key, (k, v) -> v == null ? value : v + value));

        // 实时统计弹幕人次
        Map<Integer, Double> msgUserMap = new HashMap<>(RankingCollection.msgUserMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> (double) entry.getValue().size())));
        RankingCollection.msgUserMap.clear();
        RankingCollection.msgUserSlots5.putIntoSlots(msgUserMap);
        RankingCollection.msgUserSlots60.putIntoSlots(msgUserMap);
        msgUserMap.forEach((key, value) -> RankingCollection.msgUserToday.compute(key, (k, v) -> v == null ? value : v + value));

        // 实时统计礼物数量
        Map<Integer, Double> giftCountMap = new HashMap<>(RankingCollection.giftCountMap);
        RankingCollection.giftCountMap.clear();
        RankingCollection.giftCountSlots5.putIntoSlots(giftCountMap);
        RankingCollection.giftCountSlots60.putIntoSlots(giftCountMap);
        giftCountMap.forEach((key, value) -> RankingCollection.giftCountToday.compute(key, (k, v) -> v == null ? value : v + value));

        // 实时统计礼物人次
        Map<Integer, Double> giftUserMap = new HashMap<>(RankingCollection.giftUserMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> (double) entry.getValue().size())));
        RankingCollection.giftUserMap.clear();
        RankingCollection.giftUserSlots5.putIntoSlots(giftUserMap);
        RankingCollection.giftUserSlots60.putIntoSlots(giftUserMap);
        giftUserMap.forEach((key, value) -> RankingCollection.giftUserToday.compute(key, (k, v) -> v == null ? value : v + value));

        // 实时统计礼物价值
        Map<Integer, Double> giftPriceMap = new HashMap<>(RankingCollection.giftPriceMap);
        RankingCollection.giftPriceMap.clear();
        RankingCollection.giftPriceSlots5.putIntoSlots(giftPriceMap);
        RankingCollection.giftPriceSlots60.putIntoSlots(giftPriceMap);
        giftPriceMap.forEach((key, value) -> RankingCollection.giftPriceToday.compute(key, (k, v) -> v == null ? value : v + value));

        putData(RedisKeys.DOUYU_RANK_ANCHOR_CHATMSG_COUNT_MINUTE_5, RankingCollection.msgCountSlots5);
        putData(RedisKeys.DOUYU_RANK_ANCHOR_CHATMSG_COUNT_MINUTE_60, RankingCollection.msgCountSlots60);
        putData(RedisKeys.DOUYU_RANK_ANCHOR_CHATMSG_USER_MINUTE_5, RankingCollection.msgUserSlots5);
        putData(RedisKeys.DOUYU_RANK_ANCHOR_CHATMSG_USER_MINUTE_60, RankingCollection.msgUserSlots60);

        putData(RedisKeys.DOUYU_RANK_ANCHOR_DGB_COUNT_MINUTE_5, RankingCollection.giftCountSlots5);
        putData(RedisKeys.DOUYU_RANK_ANCHOR_DGB_COUNT_MINUTE_60, RankingCollection.giftCountSlots60);
        putData(RedisKeys.DOUYU_RANK_ANCHOR_DGB_USER_MINUTE_5, RankingCollection.giftUserSlots5);
        putData(RedisKeys.DOUYU_RANK_ANCHOR_DGB_USER_MINUTE_60, RankingCollection.giftUserSlots60);
        putData(RedisKeys.DOUYU_RANK_ANCHOR_DGB_PRICE_MINUTE_5, RankingCollection.giftPriceSlots5);
        putData(RedisKeys.DOUYU_RANK_ANCHOR_DGB_PRICE_MINUTE_60, RankingCollection.giftPriceSlots60);

        putData(RedisKeys.DOUYU_RANK_ANCHOR_CHATMSG_COUNT_TODAY, RankingCollection.msgCountToday);
        putData(RedisKeys.DOUYU_RANK_ANCHOR_CHATMSG_USER_TODAY, RankingCollection.msgUserToday);
        putData(RedisKeys.DOUYU_RANK_ANCHOR_DGB_COUNT_TODAY, RankingCollection.giftCountToday);
        putData(RedisKeys.DOUYU_RANK_ANCHOR_DGB_USER_TODAY, RankingCollection.giftUserToday);
        putData(RedisKeys.DOUYU_RANK_ANCHOR_DGB_PRICE_TODAY, RankingCollection.giftPriceToday);

        if (morningMark) {
            transHistoryData();
            RankingCollection.msgCountToday.clear();
            RankingCollection.msgUserToday.clear();
            RankingCollection.giftCountToday.clear();
            RankingCollection.giftUserToday.clear();
            RankingCollection.giftPriceToday.clear();
            logger.debug("Message ranking and gift ranking cleared");
        }
    }

    private void putData(String key, RankingSlots rankingSlots) {
        RScoredSortedSet<Integer> scoredSortedSet = RedisUtil.client.getScoredSortedSet(key);
        scoredSortedSet.clear();
        rankingSlots.querySlotSum().forEach((k, v) -> scoredSortedSet.add(v, k));
    }

    private void putData(String key, Map<Integer, Double> dataMap) {
        RScoredSortedSet<Integer> scoredSortedSet = RedisUtil.client.getScoredSortedSet(key);
        scoredSortedSet.clear();
        dataMap.forEach((k, v) -> scoredSortedSet.add(v, k));
    }

    private void transHistoryData() {
        putHistoryData(RankingCollection.msgCountToday,
                RedisKeys.DOUYU_RANK_ANCHOR_MSG_COUNT_PREFIX,
                RedisKeys.SUB_MSG_COUNT);

        putHistoryData(RankingCollection.msgUserToday,
                RedisKeys.DOUYU_RANK_ANCHOR_MSG_USER_PREFIX,
                RedisKeys.SUB_MSG_USER);

        putHistoryData(RankingCollection.giftCountToday,
                RedisKeys.DOUYU_RANK_ANCHOR_DGB_COUNT_PREFIX,
                RedisKeys.SUB_DGB_COUNT);

        putHistoryData(RankingCollection.giftUserToday,
                RedisKeys.DOUYU_RANK_ANCHOR_DGB_USER_PREFIX,
                RedisKeys.SUB_DGB_USER);

        putHistoryData(RankingCollection.giftPriceToday,
                RedisKeys.DOUYU_RANK_ANCHOR_DGB_PRICE_PREFIX,
                RedisKeys.SUB_DGB_PRICE);
    }

    private void putHistoryData(Map<Integer, ?> dataMap, String rankKeyPrefix, String hashKey) {
        String yesterday = LocalDate.now().minusDays(1).format(dateFormatter);
        String rankKey = rankKeyPrefix + yesterday;
        RScoredSortedSet<Integer> scoredSortedSet = RedisUtil.client.getScoredSortedSet(rankKey);
        scoredSortedSet.clear();
        dataMap.forEach((roomId, obj) -> {
            double score = 0;
            if (obj instanceof Double) {
                score = (Double) obj;
            } else if (obj instanceof Set) {
                score = (double) ((Set) obj).size();
            } else {
                logger.error("Found obj instanceof nothing");
            }
            scoredSortedSet.add(score, roomId);
            String detailKey = RedisKeys.DOUYU_DETAIL_ANCHOR_PREFIX + roomId + ":" + yesterday;
            RedisUtil.client.getMap(detailKey).put(hashKey, score);
        });
    }
}
