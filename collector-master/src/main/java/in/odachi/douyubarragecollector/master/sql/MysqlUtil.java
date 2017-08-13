package in.odachi.douyubarragecollector.master.sql;

import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.constant.LocalFileProperties;
import in.odachi.douyubarragecollector.util.FormatterUtil;
import in.odachi.douyubarragecollector.util.LogUtil;
import org.apache.log4j.Logger;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Mysql工具类
 */
public class MysqlUtil {

    private static final Logger logger = Logger.getLogger(MysqlUtil.class);

    private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(Constants.DATETIME_PATTERN);

    private static Connection conn;

    static {
        try {
            conn = DriverManager.getConnection(LocalFileProperties.getMysqlJdbcUrl(),
                    LocalFileProperties.getMysqlUsername(), LocalFileProperties.getMysqlPassword());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 批量查询房间数据
     */
    public static Map<String, Object> queryRoom(Integer roomId) {
        Map<String, Object> roomMap = new HashMap<>(9);
        try (PreparedStatement pStmtRoom = conn.prepareStatement(MysqlConstants.SELECT_TABLE_ROOM)) {
            pStmtRoom.setInt(1, roomId);
            ResultSet rs = pStmtRoom.executeQuery();
            if (rs.next()) {
                roomMap.put("room_id", rs.getInt("room_id"));
                roomMap.put("cate_id", rs.getString("cate_id"));
                roomMap.put("cate_name", rs.getString("cate_name"));
                roomMap.put("room_name", rs.getString("room_name"));
                roomMap.put("owner_name", rs.getString("owner_name"));
                roomMap.put("owner_weight", rs.getString("owner_weight"));
                roomMap.put("fans_num", rs.getInt("fans_num"));
                roomMap.put("avatar", rs.getString("avatar"));
                roomMap.put("date_time", rs.getString("date_time"));
            }
        } catch (RuntimeException | SQLException e) {
            logger.error(LogUtil.printStackTrace(e));
        }
        return roomMap;
    }

    /**
     * 批量查询礼物数据
     */
    public static Map<String, Object> queryGift(Integer giftId) {
        Map<String, Object> giftMap = new HashMap<>(9);
        try (PreparedStatement pStmtRoom = conn.prepareStatement(MysqlConstants.SELECT_TABLE_GIFT)) {
            pStmtRoom.setInt(1, giftId);
            ResultSet rs = pStmtRoom.executeQuery();
            if (rs.next()) {
                giftMap.put("id", rs.getInt("id"));
                giftMap.put("name", rs.getString("name"));
                giftMap.put("type", rs.getString("type"));
                giftMap.put("pc", rs.getInt("pc"));
                giftMap.put("gx", rs.getInt("gx"));
                giftMap.put("desc", rs.getString("desc"));
                giftMap.put("intro", rs.getString("intro"));
                giftMap.put("mimg", rs.getString("mimg"));
                giftMap.put("himg", rs.getString("himg"));
            }
        } catch (RuntimeException | SQLException e) {
            logger.error(LogUtil.printStackTrace(e));
        }
        return giftMap;
    }

    /**
     * 批量插入房间数据
     */
    public static void putRoom(Map<String, Object> param) {
        try (PreparedStatement pStmtRoom = conn.prepareStatement(MysqlConstants.INSERT_TABLE_ROOM)) {
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
            pStmtRoom.execute();
        } catch (RuntimeException | SQLException e) {
            logger.error(LogUtil.printStackTrace(e));
        }
    }

    /**
     * 批量插入礼物数据
     */
    public static void putGift(Map<String, Object> param) {
        try (PreparedStatement pStmtGift = conn.prepareStatement(MysqlConstants.INSERT_TABLE_GIFT)) {
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
            pStmtGift.execute();
        } catch (RuntimeException | SQLException e) {
            logger.error(LogUtil.printStackTrace(e));
        }
    }
}
