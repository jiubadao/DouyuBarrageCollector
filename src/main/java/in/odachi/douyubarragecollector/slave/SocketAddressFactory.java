package in.odachi.douyubarragecollector.slave;

import in.odachi.douyubarragecollector.constant.Constants;
import org.apache.log4j.Logger;

import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;

/**
 * 选择不同的域名和端口创建连接
 * 可用域名：openbarrage.douyutv.com,danmu.douyutv.com
 * 可用端口：8601,8602,12601,12602,12603,12604
 */
class SocketAddressFactory {

    private static final Logger logger = Logger.getLogger(SocketAddressFactory.class);

    // 可用域名
    private static String[] domains = new String[]{"openbarrage.douyutv.com", "danmu.douyutv.com"};

    // 可用端口
    private static Integer[] ports = new Integer[]{8601, 8602, 12601, 12602, 12603, 12604};

    private static Deque<Map.Entry<String, Integer>> entryList = new ArrayDeque<>();

    static {
        // 将所有可用连接放到列表里供访问
        for (String domain : domains) {
            for (Integer port : ports) {
                for (int i = 0; i < Constants.CONNECTION_LIMIT; i++) {
                    entryList.addLast(new AbstractMap.SimpleImmutableEntry<>(domain, port));
                }
            }
        }
    }

    /**
     * 申请一个连接
     */
    static synchronized Map.Entry<String, Integer> acquireAddressEntry() {
        if (entryList.size() <= 0) {
            throw new NullPointerException("Address entry list is empty, acquire address entry FAILED.");
        }
        Map.Entry<String, Integer> entry = entryList.removeFirst();
        logger.debug("Acquire address entry SUCCESS: " + entry + " (" + entryList.size() + ")");
        return entry;
    }

    /**
     * 归还一个连接
     */
    static synchronized void releaseAddressEntry(Map.Entry<String, Integer> addressEntry) {
        entryList.addFirst(addressEntry);
        logger.debug("Release address entry SUCCESS: " + addressEntry + " (" + entryList.size() + ")");
    }
}
