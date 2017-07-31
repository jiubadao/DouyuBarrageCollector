package in.odachi.douyubarragecollector.master.ranking;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 滑动窗口，每1分钟一格
 */
public class RankingSlots {

    private final List<Map<Integer, Double>> slots;

    // 当前索引下标
    private int index = 0;

    /**
     * 初始化必须提供大小
     * 每1分钟一格，如果统计最近5分钟，则size=5，以此类推
     */
    public RankingSlots(int size) {
        this.slots = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            this.slots.add(new HashMap<>());
        }
    }

    /**
     * 将数据放入Slot
     */
    public void putIntoSlots(Map<Integer, Double> data) {
        slots.set(index, data);
        if (++index >= slots.size()) {
            index = 0;
        }
    }

    /**
     * 查询的结果集
     */
    public Map<Integer, Double> querySlotSum() {
        return slots.stream()
                .flatMap(m -> m.entrySet().stream())
                .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.summingDouble(Map.Entry::getValue)));
    }
}
