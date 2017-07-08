package in.odachi.douyubarragecollector.master.job;

import com.jcabi.aspects.Loggable;
import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.master.Tokenizer;
import in.odachi.douyubarragecollector.master.WordSlots;
import in.odachi.douyubarragecollector.util.RedisUtil;
import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;

import java.util.HashMap;
import java.util.Map;

/**
 * 实时数据汇总任务
 */
@DisallowConcurrentExecution
public class TokenizerJob implements Job {

    private static final Logger logger = Logger.getLogger(TokenizerJob.class);

    // 5分钟实时统计
    private static WordSlots slotsMinute5 = new WordSlots(5);

    // 60分钟实时统计
    private static WordSlots slotsMinute60 = new WordSlots(60);

    // 24小时实时统计
    private static WordSlots slotsHour24 = new WordSlots(60 * 24);

    // 5分钟实时统计缓存
    private static RScoredSortedSet<String> cacheMinute5;

    // 60分钟实时统计缓存
    private static RScoredSortedSet<String> cacheMinute60;

    static {
        // 连接缓存服务
        RedissonClient redisson = RedisUtil.redisson;
        cacheMinute5 = redisson.getScoredSortedSet("douyu:analysis:keyword:minute:5:all");
        cacheMinute60 = redisson.getScoredSortedSet("douyu:analysis:keyword:minute:60:all");

        logger.info("Init keyword from redis.");
        cacheMinute5.entryRange(0, Constants.KEYWORD_MAX_COUNT).forEach(
                entry -> slotsMinute5.putInSlots(entry.getValue(), entry.getScore()));
        cacheMinute60.entryRange(0, Constants.KEYWORD_MAX_COUNT).forEach(
                entry -> slotsMinute60.putInSlots(entry.getValue(), entry.getScore()));
    }

    @Override
    @Loggable
    public void execute(JobExecutionContext context) throws JobExecutionException {
        Map<String, Double> copy = new HashMap<>(Tokenizer.tokenizerMap);
        Tokenizer.tokenizerMap.clear();
        slotsMinute5.putInSlots(copy);
        slotsMinute60.putInSlots(copy);
        slotsHour24.putInSlots(copy);

        // 缓存实时结果
        cacheMinute5.clear();
        slotsMinute5.queryTopWords(Constants.KEYWORD_MAX_COUNT).forEach(
                entry -> cacheMinute5.add(entry.getValue(), entry.getKey()));
        cacheMinute60.clear();
        slotsMinute60.queryTopWords(Constants.KEYWORD_MAX_COUNT).forEach(
                entry -> cacheMinute60.add(entry.getValue(), entry.getKey()));
    }
}
