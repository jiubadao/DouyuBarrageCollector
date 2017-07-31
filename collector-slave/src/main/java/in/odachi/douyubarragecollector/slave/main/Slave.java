package in.odachi.douyubarragecollector.slave.main;

import in.odachi.douyubarragecollector.constant.RedisKeys;
import in.odachi.douyubarragecollector.slave.client.ChannelFactory;
import in.odachi.douyubarragecollector.slave.client.ReactorManager;
import in.odachi.douyubarragecollector.util.RedisUtil;
import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;

public class Slave {

    private static final Logger logger = Logger.getLogger(Slave.class);

    private static BlockingQueue<Integer> taskQueue = RedisUtil.client.getBlockingQueue(RedisKeys.DOUYU_SYSTEM_APPEND_ROOM);

    public void start() {
        // 启动所有选择器线程
        ReactorManager reactorManager = new ReactorManager();
        // 从节点开始接受任务
        startTaskAcceptor(reactorManager, taskQueue);
        // 注册优雅停机
        shutdownGraceful(reactorManager);
    }

    private void startTaskAcceptor(ReactorManager reactorManager, BlockingQueue<Integer> taskQueue) {
        new Thread(() -> {
            logger.info(Thread.currentThread().getName() + " thread start.");
            while (true) {
                try {
                    while (!ChannelFactory.isChannelLeft()) {
                        logger.trace("NO address entry left.");
                        Thread.sleep(10 * 1000L);
                    }
                    reactorManager.addChannel(taskQueue.take());
                } catch (InterruptedException e) {
                    logger.info(Thread.currentThread().getName() + " thread exit.");
                    break;
                }
            }
        }, "TaskAcceptor").start();
    }

    private void shutdownGraceful(ReactorManager reactorManager) {
        // 优雅停机
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    logger.info("Exit signal is received, program will exit.");
                    // 中断IO线程
                    new Thread(reactorManager::interruptAll).start();

                    for (int i = 0; i < 30; i++) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ignored) {
                        }
                        if (reactorManager.isTerminated()) {
                            logger.info("ReactorManager and consumer is TERMINATED, ShutdownHook exit.");
                            return;
                        }
                    }
                    logger.info("Exit timeout, program will exit forcibly.");
                }, "ShutdownSlaveHook")
        );
    }

    public static void main(String[] args) {
        new Slave().start();
    }
}
