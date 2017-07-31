package in.odachi.douyubarragecollector.master.job;

import com.jcabi.aspects.Loggable;
import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.constant.RedisKeys;
import in.odachi.douyubarragecollector.master.client.WebFetcher;
import in.odachi.douyubarragecollector.master.util.MasterUtil;
import in.odachi.douyubarragecollector.util.FormatterUtil;
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
 * 在线时长标记
 */
@DisallowConcurrentExecution
public class TimeMarkJob implements Job {

    private static final Logger logger = Logger.getLogger(TimeMarkJob.class);

    private DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(Constants.DATE_PATTERN);

    @Override
    @Loggable
    public void execute(JobExecutionContext context) throws JobExecutionException {
        String today = LocalDate.now().format(dateFormatter);
        WebFetcher.fetchRoomInfo(MasterUtil.getListenedRoomIds()).forEach(room -> {
            // 找出在线的房间累加在线时长
            Integer roomId = FormatterUtil.parseInt(room.get("room_id"));
            if ("1".equals(room.get("room_status"))) {
                Map<String, Double> onlineMap = RedisUtil.client.getMap(RedisKeys.DOUYU_ONLINE_ANCHOR_PREFIX + roomId);
                onlineMap.compute(today, (k, v) -> v == null ? 0.1 : v + 0.1);
            } else {
                logger.debug("Listened room is NOT online: " + roomId);
            }
        });
    }
}
