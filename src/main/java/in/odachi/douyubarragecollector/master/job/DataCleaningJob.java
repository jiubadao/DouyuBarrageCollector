package in.odachi.douyubarragecollector.master.job;

import com.jcabi.aspects.Loggable;
import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.sql.SqliteUtil;
import in.odachi.douyubarragecollector.util.RedisUtil;
import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.redisson.api.RKeys;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * 清理任务
 */
@DisallowConcurrentExecution
public class DataCleaningJob implements Job {

    private static final Logger logger = Logger.getLogger(DataCleaningJob.class);

    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(Constants.DATE_PATTERN);

    @Override
    @Loggable
    public void execute(JobExecutionContext context) throws JobExecutionException {
        // 创建明天的表
        SqliteUtil.createTableDaily(LocalDate.now().plusDays(1).format(dateFormatter));

        // 删除历史的日期数据
        String dateLastMonth = LocalDate.now().minusDays(Constants.DATA_KEEP_DAYS).format(dateFormatter);
        RKeys keys = RedisUtil.redisson.getKeys();
        long delCount = keys.deleteByPattern("*:" + dateLastMonth);
        logger.info("DELETE: *:" + dateLastMonth + ", count: " + delCount);

        // 删除数据库相关表
        SqliteUtil.dropTable("DGB_" + dateLastMonth);
        SqliteUtil.dropTable("MSG_" + dateLastMonth);
    }
}
