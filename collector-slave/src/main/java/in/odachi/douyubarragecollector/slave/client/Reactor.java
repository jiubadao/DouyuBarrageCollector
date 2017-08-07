package in.odachi.douyubarragecollector.slave.client;

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
class Reactor extends Thread {

    private static final Logger logger = Logger.getLogger(Reactor.class);

    private static final AtomicInteger nextId = new AtomicInteger();

    private static final int EMPTY_SELECT_MIN_COUNT = 512;

    private static final int SELECT_WAIT_TIME = 10 * 1000;

    private final ReactorManager reactorManager;

    private volatile Selector selector;

    // 待办操作列表
    private ConcurrentLinkedQueue<OpsRequest> opsRequests = new ConcurrentLinkedQueue<>();

    /**
     * 客户端初始化
     */
    Reactor(ReactorManager reactorManager) {
        this.reactorManager = reactorManager;
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
                            if (key.isValid()) {
                                key.interestOps(request.ops);
                            }
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
                    } catch (IOException | RuntimeException e) {
                        EventHandler handler = (EventHandler) key.attachment();
                        logger.warn(e + ", " + handler.toString());
                        reactorManager.closeChannel(key, true);
                    }
                }
            } catch (IOException e) {
                logger.error(LogUtil.printStackTrace(e));
                reactorManager.reOpenSelector();
            } catch (InterruptedException e) {
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
        Set<Integer> oldRoomIds = new HashSet<>();
        oldSelector.keys().forEach(key -> {
            // cancel the old key
            EventHandler handler = (EventHandler) key.attachment();
            reactorManager.closeChannel(key, false);
            oldRoomIds.add(handler.getRoomId());
        });
        logger.error("SelectionKey count in oldSelector: " + oldRoomIds.size());

        try {
            oldSelector.close();
            logger.error("Close oldSelector SUCCESS.");
        } catch (Throwable t) {
            logger.error("FAILED to close a selector.", t);
        }

        try {
            Thread.sleep(10 * 1000L);
        } catch (InterruptedException ignored) {
        }
        oldRoomIds.forEach(reactorManager::addChannel);
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
        selector.keys().forEach(key -> interestOps(key, SelectionKey.OP_WRITE));
    }

    /**
     * 连接注册事件
     */
    static final class OpsRequest {
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
