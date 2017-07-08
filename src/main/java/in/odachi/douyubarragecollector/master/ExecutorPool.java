package in.odachi.douyubarragecollector.master;

import in.odachi.douyubarragecollector.constant.Constants;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 程序共用线程池
 */
public class ExecutorPool {

    private static final Logger logger = Logger.getLogger(ExecutorPool.class);

    // 初始化线程池
    private static ExecutorService executor = Executors.newFixedThreadPool(Constants.REQUEST_THREAD_COUNT);

    /**
     * 批量执行任务
     */
    public static <T> List<Future<T>> invokeAll(List<Callable<T>> tasks) {
        List<Future<T>> futureList = new ArrayList<>();
        try {
            futureList = executor.invokeAll(tasks);
        } catch (InterruptedException e) {
            logger.warn(e);
        }
        return futureList;
    }

    /**
     * 关闭线程池
     */
    public static void shutdown() {
        executor.shutdownNow();
    }
}
