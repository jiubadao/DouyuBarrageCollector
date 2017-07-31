package in.odachi.douyubarragecollector.util;

import in.odachi.douyubarragecollector.constant.LocalFileProperties;
import in.odachi.douyubarragecollector.intf.Message;
import org.apache.commons.lang3.StringUtils;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 创建连接
 */
public class RedisUtil {

    public static RedissonClient client = createRedissonClient();

    private static RedissonClient createRedissonClient() {
        Config config = new Config();
        SingleServerConfig singleServerConfig = config.useSingleServer()
                .setAddress(LocalFileProperties.getRedisAddress());
        String redisPassword = LocalFileProperties.getRedisPassword();
        if (StringUtils.isNotBlank(redisPassword)) {
            singleServerConfig.setPassword(redisPassword);
        }
        singleServerConfig.setDatabase(LocalFileProperties.getRedisDb());
        return Redisson.create(config);
    }
}
