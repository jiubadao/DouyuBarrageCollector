package in.odachi.douyubarragecollector.sql;

import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.constant.LocalFileProperties;
import in.odachi.douyubarragecollector.util.FormatterUtil;
import in.odachi.douyubarragecollector.util.LogUtil;
import org.apache.log4j.Logger;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Sqlite工具类
 */
public class SqliteUtil {

    private static final Logger logger = Logger.getLogger(SqliteUtil.class);

    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(Constants.DATE_PATTERN);

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(Constants.DATETIME_PATTERN);

    static  {
        // 创建房间表和礼物表
        createTable();
        // 启动时创建当天的表
        createTableDaily(LocalDate.now().format(dateFormatter));
        // 启动时创建明天的表
        createTableDaily(LocalDate.now().plusDays(1).format(dateFormatter));
    }

    /**
     * 获取连接
     */
    private static synchronized Connection getConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(LocalFileProperties.getSqliteJdbcUrl());
        conn.setAutoCommit(false);
        return conn;
    }

    /**
     * 批量插入弹幕数据
     */
    public static synchronized void commitChartMsgList(Collection<Map<String, Object>> paramList) {
        String date = LocalDate.now().format(dateFormatter);
        try (Connection conn = getConnection();
             PreparedStatement pStmtChatMsg = conn.prepareStatement(SqliteConstants.INSERT_TABLE_MSG.replace("#date", date))) {
            paramList.forEach(param -> {
                try {
                    pStmtChatMsg.setInt(1, FormatterUtil.parseInt(param.get("rid")));
                    pStmtChatMsg.setInt(2, FormatterUtil.parseInt(param.get("uid")));
                    pStmtChatMsg.setString(3, String.valueOf(param.get("nn")));
                    pStmtChatMsg.setString(4, String.valueOf(param.get("txt")));
                    pStmtChatMsg.setString(5, String.valueOf(param.get("cid")));
                    pStmtChatMsg.setInt(6, FormatterUtil.parseInt(param.get("level")));
                    pStmtChatMsg.setString(7, String.valueOf(param.get("date_time")));
                    pStmtChatMsg.addBatch();
                } catch (RuntimeException | SQLException e) {
                    logger.error(param);
                    logger.error(LogUtil.printStackTrace(e));
                }
            });
            int[] chatMsgRt = pStmtChatMsg.executeBatch();
            conn.commit();
            logger.trace("Msg batch committed: " + chatMsgRt.length);
        } catch (RuntimeException | SQLException e) {
            logger.error(LogUtil.printStackTrace(e));
        }
    }

    /**
     * 批量插入礼物数据
     */
    public static synchronized void commitDgbList(Collection<Map<String, Object>> paramList) {
        String date = LocalDate.now().format(dateFormatter);
        try (Connection conn = getConnection();
             PreparedStatement pStmtDgb = conn.prepareStatement(SqliteConstants.INSERT_TABLE_DGB.replace("#date", date))) {
            paramList.forEach(param -> {
                try {
                    pStmtDgb.setInt(1, FormatterUtil.parseInt(param.get("rid")));
                    pStmtDgb.setInt(2, FormatterUtil.parseInt(param.get("gfid")));
                    pStmtDgb.setInt(3, FormatterUtil.parseInt(param.get("uid")));
                    pStmtDgb.setString(4, String.valueOf(param.get("nn")));
                    pStmtDgb.setInt(5, FormatterUtil.parseInt(param.get("level")));
                    pStmtDgb.setInt(6, FormatterUtil.parseInt(param.get("hits")));
                    pStmtDgb.setString(7, String.valueOf(param.get("date_time")));
                    pStmtDgb.addBatch();
                } catch (RuntimeException | SQLException e) {
                    logger.error(param);
                    logger.error(LogUtil.printStackTrace(e));
                }
            });
            int[] dgbRt = pStmtDgb.executeBatch();
            conn.commit();
            logger.trace("Dgb batch committed: " + dgbRt.length);
        } catch (RuntimeException | SQLException e) {
            logger.error(LogUtil.printStackTrace(e));
        }
    }

    /**
     * 批量插入房间数据
     */
    public static synchronized void commitRoomList(Collection<Map<String, Object>> paramList) {
        try (Connection conn = getConnection();
             PreparedStatement pStmtRoom = conn.prepareStatement(SqliteConstants.INSERT_TABLE_ROOM)) {
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
    public static synchronized void commitGiftList(Collection<Map<String, Object>> paramList) {
        try (Connection conn = getConnection();
             PreparedStatement pStmtGift = conn.prepareStatement(SqliteConstants.INSERT_TABLE_GIFT)) {
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

    /**
     * 创建表
     */
    private static synchronized void createTable() {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(SqliteConstants.CREATE_TABLE_ROOM);
            stmt.execute(SqliteConstants.CREATE_TABLE_GIFT);
            conn.commit();
        } catch (RuntimeException | SQLException e) {
            logger.error(LogUtil.printStackTrace(e));
        }
    }

    /**
     * 创建表
     */
    public static synchronized void createTableDaily(String date) {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(SqliteConstants.CREATE_TABLE_MSG.replace("#date", date));
            stmt.execute(SqliteConstants.CREATE_TABLE_DGB.replace("#date", date));
            conn.commit();
            logger.info("Table created: " + date);
        } catch (RuntimeException | SQLException e) {
            logger.error(LogUtil.printStackTrace(e));
        }
    }

    /**
     * 批量查询排名数据
     */
    public static synchronized List<Map<String, Object>> select(String sql, String date) {
        List<Map<String, Object>> rankList = new ArrayList<>();
        try (Connection conn = getConnection(); Statement statement = conn.createStatement()) {
            ResultSet rs = statement.executeQuery(sql.replace("#date", date));
            logger.debug(sql.replace("#date", date));
            while (rs.next()) {
                Map<String, Object> rankMap = new HashMap<>(2);
                rankMap.put("rid", rs.getString("rid"));
                rankMap.put("c", rs.getString("c"));
                rankList.add(rankMap);
            }
        } catch (RuntimeException | SQLException e) {
            logger.error(LogUtil.printStackTrace(e));
        }
        return rankList;
    }

    /**
     * 批量查询排名数据
     */
    public static synchronized List<Map<String, Object>> selectRoomList() {
        List<Map<String, Object>> roomList = new ArrayList<>();
        try (Connection conn = getConnection(); Statement statement = conn.createStatement()) {
            ResultSet rs = statement.executeQuery(SqliteConstants.SELECT_TABLE_ROOM);
            while (rs.next()) {
                Map<String, Object> roomMap = new HashMap<>(8);
                roomMap.put("room_id", rs.getInt("room_id"));
                roomMap.put("cate_id", rs.getString("cate_id"));
                roomMap.put("cate_name", rs.getString("cate_name"));
                roomMap.put("room_name", rs.getString("room_name"));
                roomMap.put("owner_name", rs.getString("owner_name"));
                roomMap.put("owner_weight", rs.getString("owner_weight"));
                roomMap.put("fans_num", rs.getInt("fans_num"));
                roomMap.put("avatar", rs.getInt("avatar"));
                roomMap.put("date_time", rs.getString("date_time"));
                roomList.add(roomMap);
            }
        } catch (RuntimeException | SQLException e) {
            logger.error(LogUtil.printStackTrace(e));
        }
        return roomList;
    }

    /**
     * 批量查询排名数据
     */
    public static synchronized List<Map<String, Object>> selectGiftList() {
        List<Map<String, Object>> giftList = new ArrayList<>();
        try (Connection conn = getConnection(); Statement statement = conn.createStatement()) {
            ResultSet rs = statement.executeQuery(SqliteConstants.SELECT_TABLE_GIFT);
            while (rs.next()) {
                Map<String, Object> giftMap = new HashMap<>(9);
                giftMap.put("id", rs.getInt("id"));
                giftMap.put("name", rs.getString("name"));
                giftMap.put("type", rs.getString("type"));
                giftMap.put("pc", rs.getInt("pc"));
                giftMap.put("gx", rs.getInt("gx"));
                giftMap.put("desc", rs.getString("desc"));
                giftMap.put("intro", rs.getString("intro"));
                giftMap.put("mimg", rs.getString("mimg"));
                giftMap.put("himg", rs.getString("himg"));
                giftList.add(giftMap);
            }
        } catch (RuntimeException | SQLException e) {
            logger.error(LogUtil.printStackTrace(e));
        }
        return giftList;
    }

    /**
     * 删除指定表
     */
    public static synchronized void dropTable(String tableName) {
        try (Connection conn = getConnection(); Statement statement = conn.createStatement()) {
            String sql = SqliteConstants.DROP_TABLE.replace("#name", tableName);
            statement.execute(sql);
            conn.commit();
            logger.debug(sql);
        } catch (RuntimeException | SQLException e) {
            logger.error(LogUtil.printStackTrace(e));
        }
    }
}
