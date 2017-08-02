package in.odachi.douyubarragecollector.master.tokenizer;

import in.odachi.douyubarragecollector.master.util.MasterUtil;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TokenizerCollection {

    private static final Logger logger = Logger.getLogger(TokenizerCollection.class);

    private static Map<String, TokenizerSlots> tokenizerSlotMap = new ConcurrentHashMap<>(1000);

    private static Map<Integer, Map<String, Double>> tokenizerMap = new ConcurrentHashMap<>();

    /**
     * 获取指定房间的分词容器
     */
    public static Map<String, Double> getTokenizerMap(Integer roomId) {
        return tokenizerMap.computeIfAbsent(roomId, k -> new ConcurrentHashMap<>());
    }

    /**
     * 获取指定房间的Slots容器
     */
    public static TokenizerSlots getSlots(int size, Integer roomId) {
        return tokenizerSlotMap.computeIfAbsent(size + ":" + roomId, key -> new TokenizerSlots(size));
    }

    /**
     * 清理已下线的房间
     */
    public static void clearOfflineMap() {
        Set<Integer> listenedRoomIds = MasterUtil.getListenedRoomIds();
        tokenizerSlotMap.forEach((k, v) -> {
            Integer roomId = Integer.parseInt(k.split(":")[1]);
            if (roomId != 0 && !listenedRoomIds.contains(roomId)) {
                tokenizerSlotMap.remove(k);
                logger.debug("Room removed from tokenizerSlotMap: " + k);
            }
        });
        tokenizerMap.forEach((roomId, v) -> {
            if (roomId != 0 && !listenedRoomIds.contains(roomId)) {
                tokenizerMap.remove(roomId);
                logger.debug("Room removed from tokenizerMap: " + roomId);
            }
        });
    }
}