package in.odachi.douyubarragecollector.master.job;

import com.jcabi.aspects.Loggable;
import in.odachi.douyubarragecollector.master.LocalCache;
import in.odachi.douyubarragecollector.slave.ReactorManager;
import in.odachi.douyubarragecollector.master.Fetcher;
import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.util.FormatterUtil;
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

/**
 * 房间状态抓取任务
 */
@DisallowConcurrentExecution
public class WebSpiderJob implements Job {

    private static final Logger logger = Logger.getLogger(WebSpiderJob.class);

    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(Constants.DATETIME_PATTERN);

    @Override
    @Loggable
    public void execute(JobExecutionContext context) throws JobExecutionException {
        // 统一每个批次的抓取时间
        String dateTime = LocalDateTime.now().format(dateTimeFormatter);
        List<Map<String, Object>> roomList = Fetcher.fetchRoom();
        Set<Integer> onlineROOM = new HashSet<>(roomList.size());
        roomList.forEach(room -> {
            room.put("date_time", dateTime);
            onlineROOM.add(FormatterUtil.parseInt(room.get("room_id")));
        });
        // 将房间新增数据提交到数据库
        LocalCache.getInstance().commitChange();

        Set<Integer> listenedROOM = ReactorManager.getInstance().getListenedRoomSet();
        Set<Integer> addedRoom = new HashSet<>(onlineROOM);
        addedRoom.removeAll(listenedROOM);
        addedRoom.forEach(roomId -> ReactorManager.getInstance().addChannel(roomId, true));
    }
}
