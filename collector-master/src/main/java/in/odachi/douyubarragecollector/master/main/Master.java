package in.odachi.douyubarragecollector.master.main;

import in.odachi.douyubarragecollector.master.client.Consumer;
import in.odachi.douyubarragecollector.master.client.ExecutorPool;
import in.odachi.douyubarragecollector.master.client.JobManager;
import org.apache.log4j.Logger;

public class Master {

    private static final Logger logger = Logger.getLogger(Master.class);

    public void start() {
        // 启动消息消费者线程
        Consumer consumer = new Consumer();
        consumer.start();
        // 注册优雅停机
        shutdownGraceful(consumer);
        // 创建全部任务
        JobManager.createAllJobs();
    }

    private void shutdownGraceful(Consumer consumer) {
        // 优雅停机
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    logger.info("Exit signal is received, program will exit.");
                    new Thread(() -> {
                        // 中断监视线程
                        consumer.interruptThread();
                        // 结束线程池
                        ExecutorPool.shutdown();
                    }).start();

                    for (int i = 0; i < 30; i++) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ignored) {
                        }
                        if (consumer.isTerminated()) {
                            logger.info("Consumer is TERMINATED, ShutdownHook exit.");
                            return;
                        }
                    }
                    logger.info("Exit timeout, program will exit forcibly.");
                }, "ShutdownMasterHook")
        );
    }

    public static void main(String[] args) {
        new Master().start();
    }
}
