package in.odachi.douyubarragecollector.util;

import org.apache.http.client.fluent.Request;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * 工具类
 */
public class HttpUtil implements Callable<String> {

    private static final Logger LOG = Logger.getLogger(HttpUtil.class);

    private String url;

    /**
     * 构造方法
     */
    public HttpUtil(String url) {
        this.url = url;
    }

    @Override
    public String call() {
        try {
            return Request.Get(url)
                    .connectTimeout(5000)
                    .socketTimeout(5000)
                    .execute()
                    .returnContent()
                    .asString();
        } catch (IOException e) {
            LOG.warn(e);
            return "";
        }
    }
}
