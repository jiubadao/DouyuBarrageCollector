package in.odachi.douyubarragecollector.master.tokenizer;

import in.odachi.douyubarragecollector.master.util.MasterUtil;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public enum  TokenizerCollection {
    // 单例模式
    INSTANCE;

    private static final Logger logger = Logger.getLogger(TokenizerCollection.class);

    private Map<String, TokenizerSlots> tokenizerSlotMap = new ConcurrentHashMap<>(1000);

    private Map<Integer, Map<String, Double>> tokenizerMap = new ConcurrentHashMap<>();

    /**
     * 获取指定房间的分词容器
     */
    public Map<String, Double> getTokenizerMap(Integer roomId) {
        return tokenizerMap.compute(roomId, (k, v) -> v == null ? new ConcurrentHashMap<>() : v);
    }

    /**
     * 复制并清空指定房间的分词容器
     */
    public Map<String, Double> copyAndClearTokenizerMap(Integer roomId) {
        Map<String, Double> resultMap = new HashMap<>();
        Map<String, Double> mapTemp = tokenizerMap.compute(roomId, (k, v) -> v == null ? new ConcurrentHashMap<>() : v);
        mapTemp.keySet().forEach(rId -> resultMap.put(rId, mapTemp.replace(rId, null)));
        return resultMap;
    }

    /**
     * 获取指定房间的Slots容器
     */
    public TokenizerSlots getSlots(int size, Integer roomId) {
        return tokenizerSlotMap.compute(size + ":" + roomId, (k, v) -> v == null ? new TokenizerSlots(size) : v);
    }

    /**
     * 清理已下线的房间
     */
    public void clearOfflineMap() {
        Set<Integer> listenedRoomIds = MasterUtil.getListenedRoomIds();
        tokenizerSlotMap.keySet().forEach(roomKey -> {
            Integer roomId = Integer.parseInt(roomKey.split(":")[1]);
            if (roomId != 0 && !listenedRoomIds.contains(roomId)) {
                tokenizerSlotMap.remove(roomKey);
                logger.debug("Room removed from tokenizerSlotMap: " + roomKey);
            }
        });
        tokenizerMap.keySet().forEach(roomId -> {
            if (roomId != 0 && !listenedRoomIds.contains(roomId)) {
                tokenizerMap.remove(roomId);
                logger.debug("Room removed from tokenizerMap: " + roomId);
            }
        });
    }
}