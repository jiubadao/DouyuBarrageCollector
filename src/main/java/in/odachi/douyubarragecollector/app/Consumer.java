package in.odachi.douyubarragecollector.app;

import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.master.Tokenizer;
import in.odachi.douyubarragecollector.sql.SqliteUtil;
import in.odachi.douyubarragecollector.util.LogUtil;
import in.odachi.douyubarragecollector.util.PacketUtil;
import org.apache.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 消息队列
 */
public class Consumer extends Thread {

    private static final Logger logger = Logger.getLogger(Consumer.class);

    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(Constants.DATE_PATTERN);

    private static final class Holder {
        private static final Consumer instance = new Consumer();
    }

    /**
     * 单例
     */
    public static Consumer getInstance() {
        return Holder.instance;
    }

    private Thread watcherThread;

    private BlockingQueue<Map.Entry<String, LocalDateTime>> queue;

    private volatile long processedTotalCount = 0;

    private volatile long processedPartCount = 0;

    private List<Map<String, Object>> chatMsgBatch = new LinkedList<>();

    private List<Map<String, Object>> dgbBatch = new LinkedList<>();

    /**
     * 私有构造器
     */
    private Consumer() {
        setName(Consumer.class.getSimpleName());
        // 使用阻塞队列
        this.queue = new LinkedBlockingQueue<>();
        // 创建监视线程
        startWatcherThread();
    }

    /**
     * 启动消费者线程疯狂消费弹幕消息
     */
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Map.Entry<String, LocalDateTime> barrage;
                try {
                    barrage = queue.take();
                    logger.trace(barrage);
                } catch (InterruptedException e) {
                    break;
                }

                boolean isTodayMsg = true;
                if (!LocalDateTime.now().format(dateFormatter).equals(barrage.getValue().format(dateFormatter))) {
                    isTodayMsg = false;
                }
                if (!isTodayMsg || chatMsgBatch.size() >= Constants.BATCH_COUNT) {
                    SqliteUtil.commitChartMsgList(chatMsgBatch);
                    processedPartCount += chatMsgBatch.size();
                    chatMsgBatch.clear();
                }
                if (!isTodayMsg || dgbBatch.size() >= Constants.BATCH_COUNT) {
                    SqliteUtil.commitDgbList(dgbBatch);
                    processedPartCount += dgbBatch.size();
                    dgbBatch.clear();
                }

                String dataBodyStr = barrage.getKey();
                Map<String, Object> msgMap = PacketUtil.parseBarrageMsgToMap(dataBodyStr);
                processedTotalCount++;
                msgMap.put("date_time", barrage.getValue());
                String type = (String) msgMap.get("type");
                if ("chatmsg".equals(type)) {
                    // 弹幕消息
                    Tokenizer.segment(String.valueOf(msgMap.get("txt")));
                    chatMsgBatch.add(msgMap);
                } else if ("dgb".equals(type)) {
                    //  礼物消息
                    dgbBatch.add(msgMap);
                }
            } catch (RuntimeException e) {
                logger.error(LogUtil.printStackTrace(e));
            }
        }
        // 将剩余的消息提交数据库
        SqliteUtil.commitChartMsgList(chatMsgBatch);
        SqliteUtil.commitDgbList(dgbBatch);
        chatMsgBatch.clear();
        dgbBatch.clear();
        // 中断监视线程
        watcherThread.interrupt();
        logger.error(Thread.currentThread().getName() + " has exited.");
    }

    /**
     * 启动监视线程
     */
    private void startWatcherThread() {
        watcherThread = new Thread(() -> {
            final Long MILLIS = 60 * 1000L;
            long lastProcessedTotal = 0;
            long lastProcessedPart = 0;

            while (!Thread.currentThread().isInterrupted()) {
                long processedPart = processedPartCount + chatMsgBatch.size() + dgbBatch.size();
                String processedPartRate = String.format("%.0f", (processedPart - lastProcessedPart)
                        / (double) MILLIS * 60 * 1000);
                String processedTotalRate = String.format("%.0f", (processedTotalCount - lastProcessedTotal)
                        / (double) MILLIS * 60 * 1000);

                // 拼接输出字符串
                String builder = "Statistics, " + "barrage queue: " + queue.size() + ", " +
                        "processed total rate: " + processedTotalRate + "/minute, " +
                        "processed msg rate: " + processedPartRate + "/minute";

                // 输出到日志
                logger.info(builder);
                lastProcessedPart = processedPart;
                lastProcessedTotal = processedTotalCount;

                try {
                    Thread.sleep(MILLIS);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }, "Watcher");
        // 启动线程
        watcherThread.start();
    }

    /**
     * Getter
     */
    public BlockingQueue<Map.Entry<String, LocalDateTime>> getQueue() {
        return queue;
    }
}
