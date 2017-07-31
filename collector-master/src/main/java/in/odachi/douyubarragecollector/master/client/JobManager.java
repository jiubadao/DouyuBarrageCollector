package in.odachi.douyubarragecollector.master.client;

import in.odachi.douyubarragecollector.master.job.*;
import in.odachi.douyubarragecollector.util.LogUtil;
import org.apache.log4j.Logger;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

/**
 * 管理定时任务
 */
public class JobManager {

    private static final Logger logger = Logger.getLogger(JobManager.class);

    /**
     * 创建全部任务
     */
    public static void createAllJobs() {
        // 创建抓取任务
        createWebFetcherJob();
        // 创建历史数据清理任务
        createCleaningJob();
        // 创建关键词统计任务
        createTokenizerJob();
        // 实时排行榜分析任务
        createRankingJob();
        // 实时排行榜分析任务
        createTimeMarkJob();
    }

    /**
     * 定时抓取任务
     * 每10分钟执行一次
     */
    private static void createWebFetcherJob() {
        scheduleJob(WebFetcherJob.class, SimpleScheduleBuilder.repeatMinutelyForever(10));
    }

    /**
     * 历史数据清理任务
     * 每天5点0分执行一次
     */
    private static void createCleaningJob() {
        scheduleJob(CleaningJob.class, CronScheduleBuilder.cronSchedule("0 0 5 * * ?"));
    }

    /**
     * 热门关键词分析任务
     * 每5分钟执行一次
     */
    private static void createTokenizerJob() {
        scheduleJob(TokenizerJob.class, CronScheduleBuilder.cronSchedule("0 0/5 * * * ?"));
    }

    /**
     * 实时排行榜分析任务
     * 每1分钟执行一次
     */
    private static void createRankingJob() {
        scheduleJob(RankingJob.class, CronScheduleBuilder.cronSchedule("0 0/1 * * * ?"));
    }

    /**
     * 在线时长标记任务
     * 每6分钟执行一次
     */
    private static void createTimeMarkJob() {
        scheduleJob(TimeMarkJob.class, CronScheduleBuilder.cronSchedule("0 0/6 * * * ?"));
    }

    private static void scheduleJob(Class<? extends Job> jobClass, ScheduleBuilder<? extends Trigger> schedBuilder) {
        try {
            JobDetail job = JobBuilder.newJob(jobClass).build();
            Trigger trigger = TriggerBuilder.newTrigger().startNow().withSchedule(schedBuilder).build();
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.scheduleJob(job, trigger);
            scheduler.start();
        } catch (SchedulerException e) {
            logger.error(LogUtil.printStackTrace(e));
        }
    }
}
