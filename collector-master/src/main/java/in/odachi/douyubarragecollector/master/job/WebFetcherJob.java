package in.odachi.douyubarragecollector.master.job;

import com.jcabi.aspects.Loggable;
import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.constant.RedisKeys;
import in.odachi.douyubarragecollector.master.client.WebFetcher;
import in.odachi.douyubarragecollector.master.client.LocalCache;
import in.odachi.douyubarragecollector.master.util.MasterUtil;
import in.odachi.douyubarragecollector.util.FormatterUtil;
import in.odachi.douyubarragecollector.util.RedisUtil;
import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

/**
 * 房间状态抓取任务
 */
@DisallowConcurrentExecution
public class WebFetcherJob implements Job {

    private static final Logger logger = Logger.getLogger(WebFetcherJob.class);

    private DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(Constants.DATETIME_PATTERN);

    @Override
    @Loggable
    public void execute(JobExecutionContext context) throws JobExecutionException {
        // 统一每个批次的抓取时间
        String dateTime = LocalDateTime.now().format(dateTimeFormatter);
        List<Map<String, Object>> roomList = WebFetcher.fetchRoom();
        Set<Integer> onlineRoomIds = new HashSet<>(roomList.size());
        roomList.forEach(room -> {
            room.put("date_time", dateTime);
            onlineRoomIds.add(FormatterUtil.parseInt(room.get("room_id")));
        });

        Set<Integer> addedRoomIds = new HashSet<>(onlineRoomIds);
        addedRoomIds.removeAll(MasterUtil.getListenedRoomIds());
        logger.info("Size of rooms online: " + onlineRoomIds.size());
        logger.info("Size of rooms to add: " + addedRoomIds.size());
        BlockingQueue<Integer> addedQueue = RedisUtil.client.getBlockingQueue(RedisKeys.DOUYU_SYSTEM_APPEND_ROOM);
        addedQueue.clear();
        addedRoomIds.forEach(roomId -> {
            try {
                addedQueue.put(roomId);
            } catch (InterruptedException ignored) {
            }
        });
    }
}
