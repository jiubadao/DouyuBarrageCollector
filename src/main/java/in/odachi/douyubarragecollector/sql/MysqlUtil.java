package in.odachi.douyubarragecollector.sql;

import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.constant.LocalFileProperties;
import in.odachi.douyubarragecollector.util.FormatterUtil;
import in.odachi.douyubarragecollector.util.LogUtil;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Map;

/**
 * Mysql工具类
 */
public class MysqlUtil {

    private static final Logger logger = Logger.getLogger(MysqlUtil.class);

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(Constants.DATETIME_PATTERN);

    /**
     * 获取连接
     */
    private static Connection getConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(LocalFileProperties.getMysqlJdbcUrl(),
                LocalFileProperties.getMysqlUsername(), LocalFileProperties.getMysqlPassword());
        conn.setAutoCommit(false);
        return conn;
    }

    /**
     * 批量插入房间数据
     */
    public static void commitRoomList(Collection<Map<String, Object>> paramList) {
        try (Connection conn = getConnection();
             PreparedStatement pStmtRoom = conn.prepareStatement(MysqlConstants.INSERT_TABLE_ROOM)) {
            paramList.forEach(param -> {
                try {
                    pStmtRoom.setInt(1, FormatterUtil.parseInt(param.get("room_id")));
                    pStmtRoom.setString(2, String.valueOf(param.get("cate_id")));
                    pStmtRoom.setString(3, String.valueOf(param.get("cate_name")));
                    pStmtRoom.setString(4, String.valueOf(param.get("room_name")));
                    pStmtRoom.setString(5, String.valueOf(param.get("owner_name")));
                    pStmtRoom.setString(6, String.valueOf(param.get("owner_weight")));
                    pStmtRoom.setInt(7, FormatterUtil.parseInt(param.get("fans_num")));
                    pStmtRoom.setString(8, String.valueOf(param.get("avatar")));
                    pStmtRoom.setString(9, LocalDateTime.now().format(dateTimeFormatter));
                    pStmtRoom.setString(10, String.valueOf(param.get("room_name")));
                    pStmtRoom.setString(11, String.valueOf(param.get("owner_weight")));
                    pStmtRoom.setInt(12, FormatterUtil.parseInt(param.get("fans_num")));
                    pStmtRoom.setString(13, String.valueOf(param.get("avatar")));
                    pStmtRoom.setString(14, LocalDateTime.now().format(dateTimeFormatter));
                    pStmtRoom.addBatch();
                } catch (RuntimeException | SQLException e) {
                    logger.error(param);
                    logger.error(LogUtil.printStackTrace(e));
                }
            });
            int[] roomRt = pStmtRoom.executeBatch();
            conn.commit();
            logger.trace("Room batch committed: " + roomRt.length);
        } catch (RuntimeException | SQLException e) {
            logger.error(LogUtil.printStackTrace(e));
        }
    }

    /**
     * 批量插入礼物数据
     */
    public static void commitGiftList(Collection<Map<String, Object>> paramList) {
        try (Connection conn = getConnection();
             PreparedStatement pStmtGift = conn.prepareStatement(MysqlConstants.INSERT_TABLE_GIFT)) {
            paramList.forEach(param -> {
                try {
                    pStmtGift.setInt(1, FormatterUtil.parseInt(param.get("id")));
                    pStmtGift.setString(2, String.valueOf(param.get("name")));
                    pStmtGift.setString(3, String.valueOf(param.get("type")));
                    pStmtGift.setString(4, String.valueOf(param.get("pc")));
                    pStmtGift.setString(5, String.valueOf(param.get("gx")));
                    pStmtGift.setString(6, String.valueOf(param.get("desc")));
                    pStmtGift.setString(7, String.valueOf(param.get("intro")));
                    pStmtGift.setString(8, String.valueOf(param.get("mimg")));
                    pStmtGift.setString(9, String.valueOf(param.get("himg")));
                    pStmtGift.addBatch();
                } catch (RuntimeException | SQLException e) {
                    logger.error(param);
                    logger.error(LogUtil.printStackTrace(e));
                }
            });
            int[] giftRt = pStmtGift.executeBatch();
            conn.commit();
            logger.trace("Gift batch committed: " + giftRt.length);
        } catch (RuntimeException | SQLException e) {
            logger.error(LogUtil.printStackTrace(e));
        }
    }
}
