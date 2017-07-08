package in.odachi.douyubarragecollector.master.job;

import com.jcabi.aspects.Loggable;
import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.sql.SqliteConstants;
import in.odachi.douyubarragecollector.sql.SqliteUtil;
import in.odachi.douyubarragecollector.util.FormatterUtil;
import in.odachi.douyubarragecollector.util.RedisUtil;
import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * 每日数据汇总任务
 */
@DisallowConcurrentExecution
public class RankAnalysisJob implements Job {

    private static final Logger logger = Logger.getLogger(RankAnalysisJob.class);

    private final String douyuDetailAnchor = "douyu:detail:anchor:";

    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(Constants.DATE_PATTERN);

    @Override
    @Loggable
    public void execute(JobExecutionContext context) throws JobExecutionException {
        String yesterday = LocalDate.now().minusDays(1).format(dateFormatter);

        String chatCountKey = "douyu:rank:anchor:chatmsg:count:" + yesterday;
        List<Map<String, Object>> chatCountList = SqliteUtil.select(SqliteConstants.ANALYSIS_MSG_COUNT, yesterday);
        RScoredSortedSet<Integer> chatCountSet = RedisUtil.redisson.getScoredSortedSet(chatCountKey);
        chatCountSet.clear();
        chatCountList.forEach(rankMap -> {
            Integer rid = FormatterUtil.parseInt(rankMap.get("rid"));
            Double count = Double.parseDouble(String.valueOf(rankMap.get("c")));
            chatCountSet.add(count, rid);
            String mapKey = douyuDetailAnchor + rid + ":" + yesterday;
            RedisUtil.redisson.getMap(mapKey).put("chatmsg:count", count);
            logger.debug("redis put: " + mapKey + ", " + "chatmsg:count, " + count);
            logger.debug("redis add: " + chatCountKey + ", " + rankMap.get("rid") + ", " + count);
        });

        String chatUserKey = "douyu:rank:anchor:chatmsg:user:" + yesterday;
        List<Map<String, Object>> chatUserList = SqliteUtil.select(SqliteConstants.ANALYSIS_MSG_USER, yesterday);
        RScoredSortedSet<Integer> chatUserSet = RedisUtil.redisson.getScoredSortedSet(chatUserKey);
        chatUserSet.clear();
        chatUserList.forEach(rankMap -> {
            Integer rid = FormatterUtil.parseInt(rankMap.get("rid"));
            Double count = Double.parseDouble(String.valueOf(rankMap.get("c")));
            chatUserSet.add(count, rid);
            String mapKey = douyuDetailAnchor + rid + ":" + yesterday;
            RedisUtil.redisson.getMap(mapKey).put("chatmsg:user", count);
            logger.debug("redis put: " + mapKey + ", " + "chatmsg:user, " + count);
            logger.debug("redis add: " + chatUserKey + ", " + rankMap.get("rid") + ", " + count);
        });

        String dgbCountKey = "douyu:rank:anchor:dgb:count:" + yesterday;
        List<Map<String, Object>> dgbCountList = SqliteUtil.select(SqliteConstants.ANALYSIS_DGB_COUNT, yesterday);
        RScoredSortedSet<Integer> dgbCountSet = RedisUtil.redisson.getScoredSortedSet(dgbCountKey);
        dgbCountSet.clear();
        dgbCountList.forEach(rankMap -> {
            Integer rid = FormatterUtil.parseInt(rankMap.get("rid"));
            Double count = Double.parseDouble(String.valueOf(rankMap.get("c")));
            dgbCountSet.add(count, rid);
            String mapKey = douyuDetailAnchor + rid + ":" + yesterday;
            RedisUtil.redisson.getMap(mapKey).put("dgb:count", count);
            logger.debug("redis put: " + mapKey + ", " + "dgb:count, " + count);
            logger.debug("redis add: " + dgbCountKey + ", " + rankMap.get("rid") + ", " + count);
        });

        String dgbUserKey = "douyu:rank:anchor:dgb:user:" + yesterday;
        List<Map<String, Object>> dgbUserList = SqliteUtil.select(SqliteConstants.ANALYSIS_DGB_USER, yesterday);
        RScoredSortedSet<Integer> dgbUserSet = RedisUtil.redisson.getScoredSortedSet(dgbUserKey);
        dgbUserSet.clear();
        dgbUserList.forEach(rankMap -> {
            Integer rid = FormatterUtil.parseInt(rankMap.get("rid"));
            Double count = Double.parseDouble(String.valueOf(rankMap.get("c")));
            dgbUserSet.add(count, rid);
            String mapKey = douyuDetailAnchor + rid + ":" + yesterday;
            RedisUtil.redisson.getMap(mapKey).put("dgb:user", count);
            logger.debug("redis put: " + mapKey + ", " + "dgb:user, " + count);
            logger.debug("redis add: " + dgbUserKey + ", " + rankMap.get("rid") + ", " + count);
        });

        String dgbPriceKey = "douyu:rank:anchor:dgb:price:" + yesterday;
        List<Map<String, Object>> dgbPriceList = SqliteUtil.select(SqliteConstants.ANALYSIS_DGB_PRICE, yesterday);
        RScoredSortedSet<Integer> dgbPriceSet = RedisUtil.redisson.getScoredSortedSet(dgbPriceKey);
        dgbPriceSet.clear();
        dgbPriceList.forEach(rankMap -> {
            Integer rid = FormatterUtil.parseInt(rankMap.get("rid"));
            Double count = Double.parseDouble(String.valueOf(rankMap.get("c")));
            dgbPriceSet.add(count, rid);
            String mapKey = douyuDetailAnchor + rid + ":" + yesterday;
            RedisUtil.redisson.getMap(mapKey).put("dgb:price", count);
            logger.debug("redis put: " + mapKey + ", " + "dgb:price, " + count);
            logger.debug("redis add: " + dgbPriceKey + ", " + rankMap.get("rid") + ", " + count);
        });
    }
}
