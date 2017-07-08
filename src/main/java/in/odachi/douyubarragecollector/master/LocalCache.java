package in.odachi.douyubarragecollector.master;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.sql.MysqlUtil;
import in.odachi.douyubarragecollector.sql.SqliteUtil;
import in.odachi.douyubarragecollector.util.FormatterUtil;
import in.odachi.douyubarragecollector.util.HttpUtil;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 缓存房间信息
 */
public class LocalCache implements Serializable {

    private static final Logger logger = Logger.getLogger(LocalCache.class);

    private static final class Holder {
        private static final LocalCache instance = new LocalCache();
    }

    public static LocalCache getInstance() {
        return Holder.instance;
    }

    // 本地缓存房间关注数
    private Cache<Integer, Integer> roomCache = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.DAYS)
            .build();

    private Map<Integer, Map<String, Object>> giftCache = new HashMap<>(500);

    // 准备更新到数据库的房间
    private List<Map<String, Object>> roomAppendList = new LinkedList<>();

    // 准备更新到数据库的礼物
    private List<Map<String, Object>> giftAppendList = new LinkedList<>();

    private LocalCache() {
        initRedisMap();
    }

    private void initRedisMap() {
        SqliteUtil.selectGiftList().forEach(giftMap -> {
            Integer giftId = FormatterUtil.parseInt(giftMap.get("id"));
            giftCache.put(giftId, giftMap);
        });
        SqliteUtil.selectRoomList().forEach(roomMap -> {
            Integer roomId = FormatterUtil.parseInt(roomMap.get("room_id"));
            Integer fansNum = FormatterUtil.parseInt(roomMap.get("fans_num"));
            roomCache.put(roomId, fansNum);
        });
    }

    /**
     * 读缓存
     */
    @SuppressWarnings("unchecked")
    public Map.Entry<Integer, Integer> get(Integer roomId) {
        Integer ret = 0;
        try {
            ret = roomCache.get(roomId, () -> {
                String context = new HttpUtil(Constants.HTTP_API_ROOM + roomId).call();
                Map<String, Object> info = Fetcher.parseJson(context);
                if (info == null || "null".equals(String.valueOf(info.get("room_id")))) {
                    return 0;
                }
                roomAppendList.add(info);

                ArrayList<Map<String, Object>> giftList = (ArrayList<Map<String, Object>>) info.get("gift");
                giftList.forEach(gift -> {
                    Integer id = FormatterUtil.parseInt(gift.get("id"));
                    if (!giftCache.containsKey(id)) {
                        giftAppendList.add(gift);
                        giftCache.put(id, gift);
                    }
                });
                Integer num = FormatterUtil.parseInt(info.get("fans_num"));
                logger.debug("Room cache NOT hit: " + roomId + ", " + num);
                return num;
            });
        } catch (ExecutionException e) {
            logger.error(e);
        }
        return new AbstractMap.SimpleImmutableEntry<>(roomId, ret);
    }

    /**
     * 将新增的房间和礼物更新到数据库
     */
    public void commitChange() {
        // 将新增项提交到数据库
        MysqlUtil.commitGiftList(giftAppendList);
        SqliteUtil.commitGiftList(giftAppendList);
        giftAppendList.clear();
        SqliteUtil.commitRoomList(roomAppendList);
        MysqlUtil.commitRoomList(roomAppendList);
        roomAppendList.clear();
    }
}
