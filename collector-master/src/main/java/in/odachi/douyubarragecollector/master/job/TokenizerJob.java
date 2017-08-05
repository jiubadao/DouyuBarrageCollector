package in.odachi.douyubarragecollector.master.job;

import com.jcabi.aspects.Loggable;
import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.constant.RedisKeys;
import in.odachi.douyubarragecollector.master.tokenizer.TokenizerCollection;
import in.odachi.douyubarragecollector.master.util.MasterUtil;
import in.odachi.douyubarragecollector.util.RedisUtil;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.redisson.api.RScoredSortedSet;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * 实时数据汇总任务
 */
@DisallowConcurrentExecution
public class TokenizerJob implements Job {

    private DateTimeFormatter minuteFormatter = DateTimeFormatter.ofPattern(Constants.MINUTE_PATTERN);

    @Override
    @Loggable
    public void execute(JobExecutionContext context) throws JobExecutionException {
        MasterUtil.getListenedRoomIds().forEach(this::putTokenizerData);
        // 清理已下线的房间
        TokenizerCollection.INSTANCE.clearOfflineMap();
    }

    private void putTokenizerData(Integer roomId) {
        Map<Integer, String> lastUpdateTimeMap = RedisUtil.client.getMap(RedisKeys.DOUYU_ANALYSIS_KEYWORD_LAST_UPDATE_TIME);
        Map<String, Double> localMap = TokenizerCollection.INSTANCE.copyAndClearTokenizerMap(roomId);
        lastUpdateTimeMap.put(roomId, LocalDateTime.now().format(minuteFormatter));
        putIntoSlots(roomId, localMap);
    }

    private void putIntoSlots(Integer roomId, Map<String, Double> localMap) {
        TokenizerCollection.INSTANCE.getSlots(1, roomId).putIntoSlots(localMap);
        TokenizerCollection.INSTANCE.getSlots(12, roomId).putIntoSlots(localMap);
        cacheToRedis(1, roomId);
        cacheToRedis(12, roomId);
    }

    private void cacheToRedis(int size, Integer roomId) {
        String keyCache = RedisKeys.DOUYU_ANALYSIS_KEYWORD_MINUTE_PREFIX + size + ":" + roomId;
        RScoredSortedSet<String> cacheMinute = RedisUtil.client.getScoredSortedSet(keyCache);
        cacheMinute.clear();
        TokenizerCollection.INSTANCE.getSlots(size, roomId).queryTopWords().forEach((k, v) -> cacheMinute.add(v, k));
    }
}