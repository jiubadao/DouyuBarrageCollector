package in.odachi.douyubarragecollector.master.util;

import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.constant.RedisKeys;
import in.odachi.douyubarragecollector.master.client.LocalCache;
import in.odachi.douyubarragecollector.sql.MysqlUtil;
import in.odachi.douyubarragecollector.util.RedisUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 主节点工具类
 */
public class MasterUtil {

    private static final Logger logger = Logger.getLogger(MasterUtil.class);

    public static Set<Integer> getListenedRoomIds() {
        Set<Integer> listenedAll = new HashSet<>();
        RedisUtil.client.getKeys().getKeysByPattern(RedisKeys.DOUYU_SYSTEM_RUNNING_ROOM_PREFIX + "*").forEach(key -> {
            Set<Integer> listenedOne = RedisUtil.client.getSet(key);
            listenedAll.addAll(listenedOne);
            logger.debug("Listened key set: " + key + ", count: " + listenedOne.size());
        });
        return listenedAll;
    }

    @SuppressWarnings("unchecked")
    public static void parseGift(Integer roomId) {
        String url = Constants.HTTPS_WWW_DOUYU_COM + roomId;
        String context = new HttpUtil(url).call();

        String pattern = "\\$ROOM\\.propBatterConfig\\s?=\\s?(.+);";
        Matcher m = Pattern.compile(pattern).matcher(context);
        if (!m.find()) {
            logger.error("Matcher for pattern NOT found: " + pattern);
            return;
        }

        String jsonStr = m.group(1);
        Map<Integer, Object> jsonMap;
        ObjectMapper mapper = new ObjectMapper();
        try {
            jsonMap = mapper.readValue(jsonStr, new TypeReference<Map<Integer, Object>>() {
            });
        } catch (IOException e) {
            logger.error(e);
            return;
        }

        List<Map<String, Object>> giftList = new LinkedList<>();
        jsonMap.forEach((id, giftObj) -> {
            Map<String, Object> giftMap = (Map<String, Object>) giftObj;
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("id", id);
            resultMap.put("name", giftMap.get("name"));
            resultMap.put("type", giftMap.get("type"));
            resultMap.put("pc", 0);
            resultMap.put("gx", 0);
            resultMap.put("desc", StringUtils.EMPTY);
            resultMap.put("intro", StringUtils.EMPTY);
            resultMap.put("mimg", giftMap.get("cimg"));
            resultMap.put("himg", giftMap.get("himg"));
            giftList.add(resultMap);
        });

        LocalCache.INSTANCE.setGift(giftList);
    }
}
