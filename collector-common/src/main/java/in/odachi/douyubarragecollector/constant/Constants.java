package in.odachi.douyubarragecollector.constant;

/**
 * 常量类
 */
public class Constants {

    // 主域名
    public static final String HTTPS_WWW_DOUYU_COM = "https://www.douyu.com/";

    // 获取所有分类
    public static final String HTTPS_WWW_DOUYU_COM_DIRECTORY = "https://www.douyu.com/directory";

    // 获取分类下在播房间
    public static final String HTTP_DIRECTORY_GAME = "https://www.douyu.com/directory/game/";

    // 第三方查询API：获取直播房间详情信息
    public static final String HTTP_API_ROOM = "http://open.douyucdn.cn/api/RoomApi/room/";

    // 弹幕池分组号，海量模式使用-9999
    public static final int GROUP_ID = -9999;

    // 弹幕客户端类型设置
    public final static int MESSAGE_TYPE_CLIENT = 689;

    // 分隔符
    public static final String REGEX = ",";

    // 用于抓取房间信息的线程数量
    public static final int REQUEST_THREAD_COUNT = 10;

    // 每个端口限制连接数
    public static final int CONNECTION_LIMIT_PER_HOST = 200;

    // 实时关键词数量
    public static final int KEYWORD_MAX_COUNT = 100;

    // 统计数据保留的历史天数
    public static final int REDIS_DATA_KEEP_DAYS = 30;

    // 消息实时处理速率数据保留分钟数
    public static final int PROCESSED_RATE_KEEP_DAYS = 576;

    // 消息实时处理速率数据保留分钟数
    public static final int PROCESSED_RATE_REPORT_GAP = 5;

    // 统计线程休息时间
    public static final long WATCHER_SLEEP_TIME = 60 * 1000L;

    // 心跳线程休息时间
    public static final long KEEP_ALIVE_SLEEP_TIME = 45 * 1000L;

    // 配置文件路径
    public static final String CONF_FILE_NAME = "conf/barrage.properties";

    // 日期格式
    public static final String DATE_PATTERN = "yyyyMMdd";

    // 时间格式
    public static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

    // 分钟格式
    public static final String MINUTE_PATTERN = "yyyy-MM-dd HH:mm";
}