package in.odachi.douyubarragecollector.master.client;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.master.util.HttpUtil;
import in.odachi.douyubarragecollector.master.sql.MysqlUtil;
import in.odachi.douyubarragecollector.util.FormatterUtil;
import org.apache.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * 缓存房间信息
 */
public enum LocalCache {
    // 单例模式
    INSTANCE;

    private static final Logger logger = Logger.getLogger(LocalCache.class);

    private Cache<Integer, Integer> roomCache = CacheBuilder.newBuilder().build();

    private Cache<Integer, Map<String, Object>> giftCache = CacheBuilder.newBuilder().build();

    private DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(Constants.DATETIME_PATTERN);

    LocalCache() {
    }

    /**
     * 读缓存
     */
    public Map.Entry<Integer, Integer> queryFansNum(Integer roomId) {
        Integer ret = 0;
        try {
            ret = roomCache.get(roomId, () -> {
                Map<String, Object> room = MysqlUtil.queryRoom(roomId);
                if (room.size() > 0) {
                    String dateTime = (String) room.get("date_time");
                    try {
                        LocalDateTime lastDataTime = LocalDateTime.parse(dateTime, dateTimeFormatter);
                        if (LocalDateTime.now().minusDays(1).compareTo(lastDataTime) < 0) {
                            logger.debug("Room cache NOT hit, query through mysql: " + roomId);
                            return FormatterUtil.parseInt(room.get("fans_num"));
                        }
                    } catch (Exception ignored) {
                    }
                }
                logger.debug("Room cache NOT hit, query through html: " + roomId);
                return queryRoomJson(roomId);
            });
        } catch (ExecutionException e) {
            logger.error(e);
        }
        return new AbstractMap.SimpleEntry<>(roomId, ret);
    }

    public Integer queryRoomJson(Integer roomId) {
        // 如果距上次房间更新时间较长，则重新抓取房间
        // 如果数据库里没有该房间，则从网页抓取，并保存到数据库
        Map<String, Object> room = WebFetcher.parseJson(new HttpUtil(Constants.HTTP_API_ROOM + roomId).call());
        if (room == null) {
            return 0;
        }
        // 保存到数据库
        MysqlUtil.putRoom(room);

        // 解析礼物
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> giftList = (List<Map<String, Object>>) room.get("gift");
        setGift(giftList);
        return FormatterUtil.parseInt(room.get("fans_num"));
    }

    public Map<String, Object> getGift(Integer giftId) {
        try {
            return giftCache.get(giftId, () -> {
                Map<String, Object> gift = MysqlUtil.queryGift(giftId);
                if (gift.size() > 0) {
                    logger.debug("Gift cache NOT hit, query through mysql: " + giftId);
                    return gift;
                }
                return new HashMap<>();
            });
        } catch (ExecutionException e) {
            logger.error(e);
            return new HashMap<>();
        }
    }

    public void setGift(List<Map<String, Object>> giftList) {
        giftList.forEach(gift -> {
            MysqlUtil.putGift(gift);
            giftCache.put(FormatterUtil.parseInt(gift.get("id")), gift);
        });
    }
}
