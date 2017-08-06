package in.odachi.douyubarragecollector.master.job;

import com.jcabi.aspects.Loggable;
import in.odachi.douyubarragecollector.master.ranking.RankingCollection;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * 实时排行任务
 */
@DisallowConcurrentExecution
public class RankingJob implements Job {

    @Override
    @Loggable
    public void execute(JobExecutionContext context) throws JobExecutionException {
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
    }
}