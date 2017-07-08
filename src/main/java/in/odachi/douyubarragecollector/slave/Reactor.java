package in.odachi.douyubarragecollector.slave;

import in.odachi.douyubarragecollector.util.LogUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.channels.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 弹幕NIO客户端类
 */
public class Reactor extends Thread {

    private static final Logger logger = Logger.getLogger(Reactor.class);

    private static final AtomicInteger nextId = new AtomicInteger();

    private static final int EMPTY_SELECT_MIN_COUNT = 512;

    // 待办操作列表
    private ConcurrentLinkedQueue<OpsRequest> opsRequests = new ConcurrentLinkedQueue<>();

    private volatile Selector selector;

    private static final int SELECT_WAIT_TIME = 10 * 1000;

    /**
     * 客户端初始化
     */
    Reactor() {
        setName(Reactor.class.getSimpleName() + "-" + nextId.getAndIncrement());
        try {
            selector = Selector.open();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void run() {
        int selectReturnsImmediately = 0;
        Selector selector = this.selector;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                // 遍历并处理全部事件
                OpsRequest request;
                while ((request = opsRequests.poll()) != null) {
                    switch (request.type) {
                        case OpsRequest.CHG:
                            // 变更
                            SelectionKey key = request.channel.keyFor(selector);
                            key.interestOps(request.ops);
                            break;
                        case OpsRequest.REG:
                            // 注册
                            request.channel.register(selector, request.ops, request.eventHandler);
                            break;
                    }
                }

                long beforeSelect = System.currentTimeMillis();
                if (selector.select(SELECT_WAIT_TIME) <= 0) {
                    long timeBlocked = System.currentTimeMillis() - beforeSelect;
                    if (timeBlocked < SELECT_WAIT_TIME) {
                        // returned before the SELECT_WAIT_TIME elapsed with nothing select.
                        // this may be the cause of the jdk epoll(..) bug, so increment the counter
                        // which we use later to see if its really the jdk bug.
                        logger.debug("Selector select returns 0: cost " + timeBlocked + "ms");
                        selectReturnsImmediately++;
                    } else {
                        selectReturnsImmediately = 0;
                    }
                    if (selectReturnsImmediately >= EMPTY_SELECT_MIN_COUNT) {
                        // The selector returned immediately for EMPTY_SELECT_MIN_COUNT times in a row,
                        // so recreate one selector as it seems like we hit the
                        // famous epoll(..) jdk bug.
                        selector = recreateSelector();
                        selectReturnsImmediately = 0;
                    }
                    // try to select again
                    continue;
                } else {
                    // reset counter
                    selectReturnsImmediately = 0;
                }

                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    // 清除已处理的通道
                    keyIterator.remove();

                    // 为了程序健壮性
                    if (!key.isValid()) {
                        logger.debug("Selection key is invalid.");
                        continue;
                    }

                    EventHandler eventHandler = (EventHandler) key.attachment();
                    try {
                        int oldOps = key.interestOps();
                        int ops = oldOps;

                        if (key.isConnectable()) {
                            // 连接事件处理方法
                            eventHandler.connect(key);
                            // 注册读事件
                            ops = SelectionKey.OP_WRITE;
                        } else if (key.isReadable()) {
                            // 读事件处理方法
                            eventHandler.read(key);
                        } else if (key.isWritable()) {
                            // 写事件处理方法
                            eventHandler.write(key);
                            // 注册读事件
                            ops = SelectionKey.OP_READ;
                        }

                        if (key.isValid() && ops != oldOps) {
                            interestOps(key, ops);
                        }
                    } catch (IOException e) {
                        EventHandler handler = (EventHandler) key.attachment();
                        logger.error(e + ", " + handler.toString());
                        try {
                            ReactorManager.getInstance().closeChannel(key, true);
                        } catch (IOException ignored) {
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                logger.warn(e);
                break;
            }
        }

        try {
            selector.close();
        } catch (IOException e) {
            logger.error(LogUtil.printStackTrace(e));
        }
        logger.error(Reactor.class.getSimpleName() + " has exited.");
    }

    /**
     * Create a new selector and "transfer" all channels from the old
     * selector to the new one
     * **Rebuild selector NOT worked. recreate the channel.**
     */
    private Selector recreateSelector() throws IOException {
        Selector newSelector = Selector.open();
        Selector oldSelector = this.selector;
        this.selector = newSelector;

        // loop over all the keys that are registered with the old Selector
        // and register them with the new one
        oldSelector.keys().forEach(key -> {
            try {
                // cancel the old key
                ReactorManager.getInstance().closeChannel(key, true);
            } catch (IOException e) {
                logger.error(LogUtil.printStackTrace(e));
            }
        });
        logger.error("SelectionKey count in oldSelector: " + oldSelector.keys().size());

        try {
            oldSelector.close();
            logger.error("Close oldSelector SUCCESS.");
        } catch (Throwable t) {
            logger.error("FAILED to close a selector.", t);
        }

        logger.error("Clazz - oldSelector: " + oldSelector + ", newSelector: " + newSelector);
        logger.error("Recreated Selector because of possible jdk epoll(..) bug.");
        return newSelector;
    }

    /**
     * 注册事件
     */
    private void interestOps(SelectionKey key, int ops) {
        opsRequests.offer(new OpsRequest((SocketChannel) key.channel(), OpsRequest.CHG, ops, null));
    }

    /**
     * 获取当前SelectionKey数量
     */
    int getSelectionKeySize() {
        return selector.keys().size();
    }

    /**
     * 获取所有在监听的房间Id
     */
    Set<Integer> getListenedRoomSet() {
        Set<Integer> listenedRoomId = new HashSet<>();
        selector.keys().forEach(key -> {
            if (key.isValid()) {
                listenedRoomId.add(((EventHandler) key.attachment()).getRoomId());
            }
        });
        return listenedRoomId;
    }

    /**
     * 注册连接
     */
    void registerChannel(SocketChannel channel, EventHandler eventHandler) throws ClosedChannelException {
        // 提交到队列
        opsRequests.offer(new OpsRequest(channel, OpsRequest.REG, SelectionKey.OP_CONNECT, eventHandler));
    }

    /**
     * 通知轮训器注册写操作
     */
    void interestOpsInAll() {
        selector.keys().forEach(key -> {
            if (key.isValid()) {
                interestOps(key, SelectionKey.OP_WRITE);
            }
        });
    }

    /**
     * 连接注册事件
     */
    public static final class OpsRequest {
        /**
         * 注册事件
         */
        static final int REG = 1;

        /**
         * 改变事件
         */
        static final int CHG = 2;

        /**
         * 操作类型（注册/修改）
         */
        final int type;

        /**
         * 注册事件：读/写
         */
        final int ops;

        /**
         * 连接
         */
        final SocketChannel channel;

        /**
         * 附加信息
         */
        EventHandler eventHandler;

        OpsRequest(SocketChannel channel, int type, int ops, EventHandler eventHandler) {
            this.channel = channel;
            this.type = type;
            this.ops = ops;
            this.eventHandler = eventHandler;
        }
    }
}
