package in.odachi.douyubarragecollector.constant;

/**
 * 常量类
 */
public class Constants {

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

    // 数据库批量提交缓冲区大小
    public static final int BATCH_COUNT = 10000;

    // 每个端口限制连接数
    public static final int CONNECTION_LIMIT = 200;

    // 关键词最大数量
    public static final int KEYWORD_MAX_COUNT = 100;

    // 数据保留的历史天数
    public static final int DATA_KEEP_DAYS = 21;

    // 配置文件路径
    public static final String CONF_FILE_NAME = "conf/barrage.properties";

    // 配置文件路径
    public static final String STOP_FILE_NAME = "conf/stopword.dic";

    // 日期格式
    public static final String DATE_PATTERN = "yyyyMMdd";

    // 时间格式
    public static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
}
