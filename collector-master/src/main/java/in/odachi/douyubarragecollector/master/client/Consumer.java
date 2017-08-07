package in.odachi.douyubarragecollector.master.client;

import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.constant.RedisKeys;
import in.odachi.douyubarragecollector.intf.Message;
import in.odachi.douyubarragecollector.master.ranking.Ranking;
import in.odachi.douyubarragecollector.master.tokenizer.Tokenizer;
import in.odachi.douyubarragecollector.util.FormatterUtil;
import in.odachi.douyubarragecollector.util.LogUtil;
import in.odachi.douyubarragecollector.util.PacketUtil;
import in.odachi.douyubarragecollector.util.RedisUtil;
import org.apache.log4j.Logger;
import org.redisson.api.RScoredSortedSet;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 消息队列
 */
public class Consumer extends Thread {

    private static final Logger logger = Logger.getLogger(Consumer.class);

    private BlockingQueue<Message> queue = RedisUtil.client.getBlockingQueue(RedisKeys.DOUYU_SYSTEM_MESSAGE_QUEUE);

    private DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(Constants.DATETIME_PATTERN);

    private AtomicLong processedTotalCount = new AtomicLong();

    private AtomicLong processedPartCount = new AtomicLong();

    private Thread watcherThread;

    /**
     * 私有构造器
     */
    public Consumer() {
        setName(Consumer.class.getSimpleName());
        startWatcherThread();
    }

    /**
     * 启动消费者线程疯狂消费弹幕消息
     */
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Message message = queue.take();
                logger.trace(message);

                String dataBodyStr = message.getMessage();
                Map<String, Object> msgMap = PacketUtil.parseBarrageMsgToMap(dataBodyStr);
                processedTotalCount.incrementAndGet();
                msgMap.put("date_time", LocalDateTime.parse(message.getDateTime(), dateTimeFormatter));
                String type = (String) msgMap.get("type");
                if ("chatmsg".equals(type)) {
                    // 弹幕消息
                    processedPartCount.incrementAndGet();
                    Ranking.incrementMessage(FormatterUtil.parseInt(msgMap.get("rid")),
                            FormatterUtil.parseInt(msgMap.get("uid")));
                    Tokenizer.segment(FormatterUtil.parseInt(msgMap.get("rid")), String.valueOf(msgMap.get("txt")));
                } else if ("dgb".equals(type)) {
                    //  礼物消息
                    processedPartCount.incrementAndGet();
                    Ranking.incrementGift(FormatterUtil.parseInt(msgMap.get("rid")),
                            FormatterUtil.parseInt(msgMap.get("uid")),
                            FormatterUtil.parseInt(msgMap.get("gfid")));
                }
            } catch (InterruptedException e) {
                break;
            } catch (RuntimeException e) {
                logger.error(LogUtil.printStackTrace(e));
            }
        }
        logger.error(Thread.currentThread().getName() + " has exited.");
    }

    /**
     * 启动监视线程
     */
    private void startWatcherThread() {
        watcherThread = new Thread(() -> {
            RScoredSortedSet<String> scoredSortedSet = RedisUtil.client.getScoredSortedSet(RedisKeys.DOUYU_SYSTEM_MESSAGE_PROCESSED_RATE);
            int modIndex = 0;
            double reportRate = 0;
            while (!Thread.currentThread().isInterrupted()) {
                long start = System.currentTimeMillis();
                double processedTotalRate = processedTotalCount.getAndSet(0) /
                        (double) Constants.WATCHER_SLEEP_TIME * 60 * 1000;
                double processedPartRate = processedPartCount.getAndSet(0) /
                        (double) Constants.WATCHER_SLEEP_TIME * 60 * 1000;
                String processedTotalRateStr = String.format("%.0f", processedTotalRate);
                String processedPartRateStr = String.format("%.0f", processedPartRate);

                reportRate += processedTotalRate;
                if (modIndex % Constants.PROCESSED_RATE_REPORT_GAP == 0) {
                    // 保存到数据库
                    long timeMillis = LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                    scoredSortedSet.add(timeMillis, String.format("%.0f", reportRate / Constants.PROCESSED_RATE_REPORT_GAP) + ":" + timeMillis);
                    // 保留最近的PROCESSED_RATE_KEEP_DAYS个数据
                    int indexDel = scoredSortedSet.size() - Constants.PROCESSED_RATE_KEEP_DAYS - 1;
                    if (indexDel >= 0) {
                        scoredSortedSet.removeRangeByRank(0, indexDel);
                    }
                    reportRate = 0;
                }
                modIndex++;

                // 拼接输出字符串
                String builder = "Statistics, " + "barrage queue: " + queue.size() + ", " +
                        "processed total rate: " + processedTotalRateStr + "/minute, " +
                        "processed msg rate: " + processedPartRateStr + "/minute";
                logger.info(builder);

                try {
                    long sleepTime = Constants.WATCHER_SLEEP_TIME - ((System.currentTimeMillis() - start));
                    if (sleepTime > 0) {
                        Thread.sleep(sleepTime);
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }, "ConsumerWatcher");
        // 启动线程
        watcherThread.start();
    }

    /**
     * 中断统计线程
     */
    public void interruptThread() {
        // 中断IO线程
        interrupt();
        // 中断监视线程
        watcherThread.interrupt();
    }

    /**
     * 所有选择器线程是否已经终止
     */
    public boolean isTerminated() {
        if (getState() != Thread.State.TERMINATED) {
            logger.error(getName() + " is NOT TERMINATED.");
            return false;
        }
        if (watcherThread.getState() != Thread.State.TERMINATED) {
            logger.error("watcherThread is NOT TERMINATED.");
            return false;
        }
        return true;
    }
}
