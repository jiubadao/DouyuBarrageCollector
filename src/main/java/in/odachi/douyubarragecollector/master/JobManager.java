package in.odachi.douyubarragecollector.master;

import in.odachi.douyubarragecollector.master.job.DataCleaningJob;
import in.odachi.douyubarragecollector.master.job.RankAnalysisJob;
import in.odachi.douyubarragecollector.master.job.TokenizerJob;
import in.odachi.douyubarragecollector.master.job.WebSpiderJob;
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
     * 定时抓取任务
     * 每10分钟执行一次
     */
    public static void createWebSpiderJob() {
        JobDetail job = JobBuilder.newJob(WebSpiderJob.class).build();
        Trigger trigger = TriggerBuilder.newTrigger().startNow()
                .withSchedule(SimpleScheduleBuilder.repeatMinutelyForever(10)).build();
        scheduleJob(job, trigger);
    }

    /**
     * 排名分析任务
     * 每天4点0分执行一次
     */
    public static void createRankAnalysisJob() {
        JobDetail job = JobBuilder.newJob(RankAnalysisJob.class).build();
        Trigger trigger = TriggerBuilder.newTrigger().startNow()
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 4 * * ?")).build();
        scheduleJob(job, trigger);
    }

    /**
     * 历史数据清理任务
     * 每天5点0分执行一次
     */
    public static void createDataCleaningJob() {
        JobDetail job = JobBuilder.newJob(DataCleaningJob.class).build();
        Trigger trigger = TriggerBuilder.newTrigger().startNow()
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0 5 * * ?")).build();
        scheduleJob(job, trigger);
    }

    /**
     * 热门关键词分析任务
     * 每分钟执行一次
     */
    public static void createTokenizerJob() {
        JobDetail job = JobBuilder.newJob(TokenizerJob.class).build();
        Trigger trigger = TriggerBuilder.newTrigger().startNow()
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0/1 * * * ?")).build();
        scheduleJob(job, trigger);
    }

    private static void scheduleJob(JobDetail job, Trigger trigger) {
        try {
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.scheduleJob(job, trigger);
            scheduler.start();
        } catch (SchedulerException e) {
            logger.error(LogUtil.printStackTrace(e));
        }
    }
}
