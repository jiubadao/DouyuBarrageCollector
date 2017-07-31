package in.odachi.douyubarragecollector.master.tokenizer;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 滑动窗口，每5分钟一格
 */
public class TokenizerSlots {

    private final List<Map<String, Double>> slots;

    // 当前索引下标
    private int index = 0;

    /**
     * 初始化必须提供大小
     * 每5分钟一格，如果统计最近5分钟，则size=1，以此类推
     */
    public TokenizerSlots(int size) {
        this.slots = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            this.slots.add(new HashMap<>());
        }
    }

    /**
     * 将数据放入Slot
     */
    public void putIntoSlots(Map<String, Double> data) {
        slots.set(index, data);
        if (++index >= slots.size()) {
            index = 0;
        }
    }

    /**
     * 查询的结果集
     */
    public Map<String, Double> queryTopWords() {
        return slots.stream()
                .flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Double::sum));
    }
}
