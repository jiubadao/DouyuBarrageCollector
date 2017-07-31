package in.odachi.douyubarragecollector.slave.client;

import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.constant.RedisKeys;
import in.odachi.douyubarragecollector.util.LogUtil;
import in.odachi.douyubarragecollector.util.RedisUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reactor管理类
 */
public class ReactorManager {

    private static final Logger logger = Logger.getLogger(ReactorManager.class);

    private static final int EMPTY_MESSAGE_TIMES = 5;

    // 选择器列表
    private Reactor reactor;

    // 心跳保持线程，所有选择器仅维护一个心跳保持线程
    private Thread keepAliveThread;

    // 统计线程
    private Thread watcherThread;

    // 处理的消息总数
    private volatile AtomicLong processedTotalCount = new AtomicLong();

    private static String runningRoomKey = RedisKeys.DOUYU_SYSTEM_RUNNING_ROOM_PREFIX +
            UUID.randomUUID().toString().replace("-", "");

    // 当前在连接的房间（远程）
    private Set<Integer> runningRoomSet = RedisUtil.client.getSet(runningRoomKey);

    // 私有构造器
    public ReactorManager() {
        reactor = new Reactor(this);
        reactor.start();
        // 启动相关线程
        startKeepAliveThread();
        startWatcherThread();
        logger.info("Running room key: " + runningRoomKey);
    }

    /**
     * 初始化选择器
     */
    public void reOpenSelector() {
        if (!reactor.isAlive()) {
            logger.error("Reactor is NOT alive: " + reactor.getName());
            reactor = new Reactor(this);
            reactor.start();
            logger.info("Reactor RE-open OK: " + reactor.getName());
        }
    }

    /**
     * 创建心跳保持线程，定时发送心跳消息
     */
    private void startKeepAliveThread() {
        keepAliveThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    // 通知轮训器注册写操作
                    reactor.interestOpsInAll();
                    logger.debug("Keep alive packet is sent, SelectionKeys: " + reactor.getSelectionKeySize());
                    Thread.sleep(Constants.KEEP_ALIVE_SLEEP_TIME);
                } catch (ClosedSelectorException e) {
                    logger.error(LogUtil.printStackTrace(e));
                    reOpenSelector();
                } catch (RuntimeException e) {
                    logger.error(LogUtil.printStackTrace(e));
                } catch (InterruptedException e) {
                    break;
                }
            }
            logger.error(Thread.currentThread().getName() + " has exited.");
        }, "KeepAlive");
        // 启动线程
        keepAliveThread.start();
        logger.debug(keepAliveThread.getName() + " thread is created.");
    }

    /**
     * 启动监视线程
     */
    private void startWatcherThread() {
        watcherThread = new Thread(() -> {
            int failedTime = 0;
            while (!Thread.currentThread().isInterrupted()) {
                String processedTotalRate = String.format("%.0f", processedTotalCount.getAndSet(0) /
                        (double) Constants.WATCHER_SLEEP_TIME * 60 * 1000);

                // 拼接输出字符串
                String builder = "Statistics, " + "processed msg rate: " + processedTotalRate + "/minute";
                if (Double.parseDouble(processedTotalRate) <= 0) {
                    // 连续数次采集数据为空则重建reactor
                    failedTime++;
                    if (failedTime >= EMPTY_MESSAGE_TIMES) {
                        reOpenSelector();
                        failedTime = 0;
                        logger.error(builder);
                    }
                } else {
                    // 输出到日志
                    failedTime = 0;
                    logger.info(builder);
                }

                try {
                    Thread.sleep(Constants.WATCHER_SLEEP_TIME);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }, "ReactorWatcher");
        // 启动线程
        watcherThread.start();
        logger.debug(watcherThread.getName() + " thread is created.");
    }

    /**
     * 生成的Channel会完成登录和注册步骤
     */
    public void addChannel(Integer roomId) {
        try {
            // 获取一个闲置的域名和端口
            Map.Entry<String, Integer> entry = ChannelFactory.acquireAddressEntry();
            logger.debug("Channel is ready to append: " + roomId + " (" + entry + ", "
                    + ChannelFactory.getChannelDequeSize() + ")");

            InetSocketAddress socketAddress = new InetSocketAddress(entry.getKey(), entry.getValue());
            SocketChannel channel = SocketChannel.open();
            // 创建一个信道，并设为非阻塞模式
            channel.configureBlocking(false);
            channel.connect(socketAddress);
            EventHandler eventHandler = new EventHandler(this, roomId, entry);
            reactor.registerChannel(channel, eventHandler);
            runningRoomSet.add(roomId);
        } catch (RuntimeException | IOException e) {
            logger.error("Channel append to selector FAILED: " + LogUtil.printStackTrace(e));
        }
    }

    /**
     * 删除连接，同时归还闲置域名:端口
     */
    public void closeChannel(SelectionKey key, boolean rebuild) {
        EventHandler handler = (EventHandler) key.attachment();
        ChannelFactory.releaseAddressEntry(handler.getChannelEntry());
        try {
            key.cancel();
            key.channel().close();
        } catch (IOException ignored) {
        }
        runningRoomSet.remove(handler.getRoomId());
        logger.debug("Channel closed: " + handler + ", rebuild: " + rebuild);
        // 是否重新连接
        if (rebuild) {
            addChannel(handler.getRoomId());
        }
    }

    /**
     * 向所有选择器线程发送中断信号
     */
    public void interruptAll() {
        runningRoomSet.clear();
        // 退出前带走心跳线程
        keepAliveThread.interrupt();
        // 中断所有选择器
        reactor.interrupt();
        // 中断统计线程
        watcherThread.interrupt();
    }

    /**
     * 所有选择器线程是否已经终止
     */
    public boolean isTerminated() {
        if (keepAliveThread.getState() != Thread.State.TERMINATED) {
            logger.error("keepAliveThread is NOT TERMINATED.");
            return false;
        }
        if (reactor.getState() != Thread.State.TERMINATED) {
            logger.error(reactor.getName() + " is NOT TERMINATED.");
            return false;
        }
        return true;
    }

    /**
     * 消息计数器
     */
    public void incrementProcessedTotalCount() {
        processedTotalCount.incrementAndGet();
    }
}
