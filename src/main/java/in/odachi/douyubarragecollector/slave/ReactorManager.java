package in.odachi.douyubarragecollector.slave;

import in.odachi.douyubarragecollector.util.LogUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.*;

/**
 * Reactor管理类
 */
public class ReactorManager {

    private static final Logger logger = Logger.getLogger(ReactorManager.class);

    // 睡眠时间
    private static final int MILLIS = 45 * 1000;

    // 线程数量（处理器数量）
    private final int count;

    // 心跳保持线程，所有选择器仅维护一个心跳保持线程
    private Thread keepAliveThread;

    // 选择器列表
    private List<Reactor> reactorList;

    private int current;

    private static final class Holder {
        private static final ReactorManager instance = new ReactorManager();
    }

    /**
     * 单例
     */
    public static ReactorManager getInstance() {
        return Holder.instance;
    }

    // 私有构造器
    private ReactorManager() {
        this.count = Runtime.getRuntime().availableProcessors();
        this.reactorList = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Reactor reactor = new Reactor();
            reactor.start();
            this.reactorList.add(reactor);
        }

        logger.info("Available processors count: " + count);
        startKeepAliveThread();
    }

    /**
     * 初始化选择器
     */
    private void reOpenSelector() {
        reactorList.forEach(reactor -> {
            if (!reactor.isAlive()) {
                logger.error("Reactor is NOT alive: " + reactor.getName());
                reactor = new Reactor();
                reactor.start();
                logger.info("Reactor RE-open OK: " + reactor.getName());
            }
        });
    }

    /**
     * 创建心跳保持线程，定时发送心跳消息
     */
    private void startKeepAliveThread() {
        Runnable runnable = () -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    // 通知轮训器注册写操作
                    reactorList.forEach(Reactor::interestOpsInAll);
                    logger.debug("Keep alive packet is sent, SelectionKeys: " + getSelectionKeySize());
                    Thread.sleep(MILLIS);
                } catch (InterruptedException e) {
                    logger.warn(e);
                    break;
                } catch (ClosedSelectorException e) {
                    logger.error(LogUtil.printStackTrace(e));
                    reOpenSelector();
                } catch (RuntimeException e) {
                    logger.error(LogUtil.printStackTrace(e));
                }
            }
            logger.error(Thread.currentThread().getName() + " has exited.");
        };

        keepAliveThread = new Thread(runnable, "KeepAlive");
        keepAliveThread.start();
        logger.debug("KeepAlive thread is created.");
    }

    /**
     * 生成的Channel会完成登录和注册步骤
     */
    public void addChannel(Integer roomId, boolean isFair) {
        try {
            logger.debug("Channel is ready to append: " + roomId);
            // 获取一个闲置的域名和端口
            Map.Entry<String, Integer> entry = SocketAddressFactory.acquireAddressEntry();
            InetSocketAddress socketAddress = new InetSocketAddress(entry.getKey(), entry.getValue());
            SocketChannel channel = SocketChannel.open();
            // 创建一个信道，并设为非阻塞模式
            channel.configureBlocking(false);
            // 异步连接服务器
            channel.connect(socketAddress);
            // 取一个选择器
            Reactor reactor = getMinReactor(isFair);
            EventHandler eventHandler = new EventHandler(roomId, entry);
            // 注册该连接
            reactor.registerChannel(channel, eventHandler);
        } catch (RuntimeException | IOException e) {
            logger.error("Channel append to selector FAILED: " + LogUtil.printStackTrace(e));
        }
    }

    /**
     * 删除连接，同时归还闲置域名:端口
     */
    public void closeChannel(SelectionKey key) throws IOException {
        closeChannel(key, false);
    }

    public void closeChannel(SelectionKey key, boolean rebuild) throws IOException {
        EventHandler handler = (EventHandler) key.attachment();
        Map.Entry<String, Integer> addressEntry = handler.getAddressEntry();
        SocketAddressFactory.releaseAddressEntry(addressEntry);
        key.cancel();
        key.channel().close();
        logger.debug("Channel closed: " + handler + ", rebuild: " + rebuild);
        // 是否重新连接
        if (rebuild) {
            addChannel(handler.getRoomId(), false);
        }
    }

    /**
     * 获取所有在监听的房间号
     */
    public Set<Integer> getListenedRoomSet() {
        Set<Integer> listenedRoomId = new HashSet<>(100);
        reactorList.forEach(reactor -> listenedRoomId.addAll(reactor.getListenedRoomSet()));
        return listenedRoomId;
    }

    /**
     * 向所有选择器线程发送中断信号
     */
    public void interruptAll() {
        // 退出前带走心跳保持线程
        keepAliveThread.interrupt();
        // 中断所有选择器
        reactorList.forEach(Thread::interrupt);
    }

    /**
     * 获取当前Key数量
     */
    private Map<String, Integer> getSelectionKeySize() {
        Map<String, Integer> sizeMap = new HashMap<>(count);
        reactorList.forEach(reactor -> sizeMap.put(reactor.getName(), reactor.getSelectionKeySize()));
        return sizeMap;
    }

    /**
     * 选取一个选择器
     */
    private Reactor getMinReactor(boolean isFair) {
        if (isFair) {
            if (current >= reactorList.size()) {
                current = 0;
            }
            logger.trace("Get min reactor fair: " + current);
            return reactorList.get(current++);
        } else {
            int index = 0;
            int minSize = reactorList.get(0).getSelectionKeySize();
            for (int i = 1; i < reactorList.size(); i++) {
                if (reactorList.get(i).getSelectionKeySize() < minSize) {
                    index = i;
                }
            }
            current = index;
            logger.trace("Get min reactor NOT fair: " + index);
            return reactorList.get(index);
        }
    }

    /**
     * 所有选择器线程是否已经终止
     */
    public boolean isTerminated() {
        if (keepAliveThread.getState() != Thread.State.TERMINATED) {
            logger.error("keepAliveThread.getState() is NOT TERMINATED.");
            return false;
        }
        for (Thread thread : reactorList) {
            if (thread.getState() != Thread.State.TERMINATED) {
                logger.error(thread.getName() + ".getState() is NOT TERMINATED.");
                return false;
            }
        }
        return true;
    }
}
