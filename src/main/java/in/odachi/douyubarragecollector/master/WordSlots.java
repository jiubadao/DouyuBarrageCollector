package in.odachi.douyubarragecollector.master;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 滑动窗口
 */
public class WordSlots {

    private final int size;

    private final List<Map<String, Double>> slots;

    // 当前索引下标
    private int index = 0;

    /**
     * 初始化必须提供大小
     * 如果统计最近5分钟，则size=5，以此类推
     */
    public WordSlots(int size) {
        this.size = size;
        this.slots = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            slots.add(new HashMap<>());
        }
    }

    /**
     * 将数据放入Slot
     */
    public void putInSlots(Map<String, Double> data) {
        slots.set(index, data);
        if (++index >= size) {
            index = 0;
        }
    }

    /**
     * 将数据放入Slot
     */
    public void putInSlots(String key, Double value) {
        slots.get(index).put(key, value);
        if (++index >= size) {
            index = 0;
        }
    }

    /**
     * 查询的结果集
     */
    public List<Map.Entry<String, Double>> queryTopWords(int limit) {
        return slots.parallelStream()
                .flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Double::sum))
                .entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(limit)
                .collect(Collectors.toList());
    }
}
