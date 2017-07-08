package in.odachi.douyubarragecollector.constant;

import in.odachi.douyubarragecollector.util.FormatterUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * 通过文件读取配置项
 * 实时生效
 */
public class LocalFileProperties {

    private static Properties getProperties() {
        try {
            Properties properties = new Properties();
            properties.load(new BufferedInputStream(new FileInputStream(Constants.CONF_FILE_NAME)));
            return properties;
        } catch (RuntimeException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Getter
    public static String getSqliteJdbcUrl() {
        return getProperties().getProperty("sqlite.jdbc.url");
    }

    public static String getMysqlJdbcUrl() {
        return getProperties().getProperty("mysql.jdbc.url");
    }

    public static String getMysqlUsername() {
        return getProperties().getProperty("mysql.username");
    }

    public static String getMysqlPassword() {
        return getProperties().getProperty("mysql.password");
    }

    public static Set<String> getDirNames() {
        String dirNames = getProperties().getProperty("dir.names");
        Set<String> dirNameSet = new HashSet<>();
        if (StringUtils.isNotBlank(dirNames)) {
            List<String> dirIds = Arrays.asList(dirNames.split(Constants.REGEX));
            dirNameSet.addAll(dirIds);
        }
        return dirNameSet;
    }

    public static Set<Integer> getRoomIds() {
        String roomIds = getProperties().getProperty("room.ids");
        Set<Integer> roomIdSet = new HashSet<>();
        if (StringUtils.isNotBlank(roomIds)) {
            List<String> roomIdList = Arrays.asList(roomIds.split(Constants.REGEX));
            roomIdList.forEach(key -> roomIdSet.add(FormatterUtil.parseInt(key)));
        }
        return roomIdSet;
    }

    public static int getFansMin() {
        return FormatterUtil.parseInt(getProperties().getProperty("fans.min"));
    }

    public static int getPopularityMin() {
        return FormatterUtil.parseInt(getProperties().getProperty("popularity.min"));
    }

    public static String getRedisAddress() {
        return getProperties().getProperty("redis.address");
    }

    public static String getRedisPassword() {
        return getProperties().getProperty("redis.password");
    }

    public static int getRedisDb() {
        return FormatterUtil.parseInt(getProperties().getProperty("redis.database"));
    }
}
