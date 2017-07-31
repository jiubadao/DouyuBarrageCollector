package in.odachi.douyubarragecollector.slave.client;

import in.odachi.douyubarragecollector.constant.Constants;
import org.apache.log4j.Logger;

import java.util.AbstractMap;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * 选择不同的域名和端口创建连接
 * 可用域名：openbarrage.douyutv.com,danmu.douyutv.com
 * 可用端口：8601,8602,12601,12602,12603,12604
 */
public class ChannelFactory {

    private static final Logger logger = Logger.getLogger(ChannelFactory.class);

    // 可用域名
    private static String[] domains = new String[]{"openbarrage.douyutv.com", "danmu.douyutv.com"};

    // 可用端口
    private static Integer[] ports = new Integer[]{8601, 8602, 12601, 12602, 12603, 12604};

    private static Deque<Map.Entry<String, Integer>> channelDeque = new LinkedBlockingDeque<>();

    static {
        // 将所有可用连接放到列表里供访问
        for (String domain : domains) {
            for (Integer port : ports) {
                for (int i = 0; i < Constants.CONNECTION_LIMIT_PER_HOST; i++) {
                    channelDeque.addLast(new AbstractMap.SimpleEntry<>(domain, port));
                }
            }
        }
    }

    /**
     * 申请一个连接
     */
    public static synchronized Map.Entry<String, Integer> acquireAddressEntry() {
        Map.Entry<String, Integer> entry = channelDeque.removeFirst();
        logger.trace("Acquire address entry SUCCESS: " + entry + " (" + channelDeque.size() + ")");
        return entry;
    }

    /**
     * 归还一个连接
     */
    public static synchronized void releaseAddressEntry(Map.Entry<String, Integer> addressEntry) {
        channelDeque.addFirst(addressEntry);
        logger.trace("Release address entry SUCCESS: " + addressEntry + " (" + channelDeque.size() + ")");
    }

    /**
     * 检查是否还有可用连接
     */
    public static synchronized boolean isChannelLeft() {
        return channelDeque.size() > 0;
    }

    /**
     * 检查是否还有可用连接
     */
    public static synchronized int getChannelDequeSize() {
        return channelDeque.size();
    }
}
