package in.odachi.douyubarragecollector.app;

import in.odachi.douyubarragecollector.master.ExecutorPool;
import in.odachi.douyubarragecollector.master.JobManager;
import in.odachi.douyubarragecollector.slave.ReactorManager;
import org.apache.log4j.Logger;

/**
 * 弹幕程序启动类
 */
public class Main {

    private static final Logger logger = Logger.getLogger(Main.class);

    /**
     * 入口方法
     */
    private void collect() {
        // 启动消息消费者线程，监控线程
        Consumer.getInstance().start();

        // 创建统计任务
        JobManager.createRankAnalysisJob();
        // 创建历史数据清理任务
        JobManager.createDataCleaningJob();
        // 创建关键词统计任务
        JobManager.createTokenizerJob();
        // 创建抓取任务
        JobManager.createWebSpiderJob();

        // 优雅停机
        shutdownGraceful();
    }

    private void shutdownGraceful() {
        // 优雅停机
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    logger.info("Exit signal is received, program will exit.");

                    new Thread(() -> {
                        // 中断IO线程
                        ReactorManager.getInstance().interruptAll();
                        // 中断消费线程
                        Consumer.getInstance().interrupt();
                        // 终止线程池
                        ExecutorPool.shutdown();
                    }).start();

                    for (int i = 0; i < 30; i++) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ignored) {
                        }
                        if (Consumer.getInstance().getState() == Thread.State.TERMINATED
                                && ReactorManager.getInstance().isTerminated()) {
                            logger.info("reactorManager and consumer is TERMINATED, ShutdownHook exit.");
                            return;
                        }
                    }
                    logger.info("Exit timeout, program will exit forcibly.");
                }, "ShutdownHook")
        );
    }

    /**
     * 入口方法
     */
    public static void main(String[] args) {
        new Main().collect();
    }
}
