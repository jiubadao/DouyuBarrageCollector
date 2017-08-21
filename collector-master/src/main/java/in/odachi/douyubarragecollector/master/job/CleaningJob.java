package in.odachi.douyubarragecollector.master.job;

import com.jcabi.aspects.Loggable;
import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.constant.RedisKeys;
import in.odachi.douyubarragecollector.util.RedisUtil;
import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * 清理任务
 */
@DisallowConcurrentExecution
public class CleaningJob implements Job {

    private static final Logger logger = Logger.getLogger(CleaningJob.class);

    private DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(Constants.DATE_PATTERN);

    @Override
    @Loggable
    public void execute(JobExecutionContext context) throws JobExecutionException {
        // 删除历史的日期数据
        String dateHistoryRedis = LocalDate.now().minusDays(Constants.REDIS_DATA_KEEP_DAYS).format(dateFormatter);
        long delCount = RedisUtil.client.getKeys().deleteByPattern("*:" + dateHistoryRedis);
        logger.info("Drop keys: *:" + dateHistoryRedis + ", count: " + delCount);

        // 删除在线时长统计历史数据
        String historyDate = LocalDate.now().minusDays(31).format(dateFormatter);
        RedisUtil.client.getKeys().getKeysByPattern(RedisKeys.DOUYU_ONLINE_ANCHOR_PREFIX + "*")
                .forEach(key -> RedisUtil.client.getMap(key).remove(historyDate));
        // 删除人气峰值统计历史数据
        RedisUtil.client.getKeys().getKeysByPattern(RedisKeys.DOUYU_POPULARITY_PEAK_ANCHOR_PREFIX + "*")
                .forEach(key -> RedisUtil.client.getMap(key).remove(historyDate));
        logger.info("Drop keys on date: " + historyDate);
    }
}
