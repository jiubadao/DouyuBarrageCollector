package in.odachi.douyubarragecollector.master.job;

import com.jcabi.aspects.Loggable;
import in.odachi.douyubarragecollector.master.ranking.RankingCollection;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.time.LocalDateTime;

/**
 * 实时排行任务
 */
@DisallowConcurrentExecution
public class RankingJob implements Job {

    @Override
    @Loggable
    public void execute(JobExecutionContext context) throws JobExecutionException {
        // 每天凌晨清空当天的统计数据，并将结果统计到历史数据中
        boolean morningMark = false;
        if (LocalDateTime.now().getHour() == 0 && LocalDateTime.now().getMinute() == 0) {
            morningMark = true;
        }

        // 实时统计弹幕数量
        RankingCollection.INSTANCE.computeAndClearMsgCount();
        // 实时统计弹幕人次
        RankingCollection.INSTANCE.computeAndClearMsgUser();
        // 实时统计礼物数量
        RankingCollection.INSTANCE.computeAndClearGiftCount();
        // 实时统计礼物人次
        RankingCollection.INSTANCE.computeAndClearGiftUser();
        // 实时统计礼物价值
        RankingCollection.INSTANCE.computeAndClearGiftPrice();

        // 缓存到数据库
        RankingCollection.INSTANCE.cacheToRedis();
        if (morningMark) {
            RankingCollection.INSTANCE.saveAndClearHistoryData();
        }
    }
}