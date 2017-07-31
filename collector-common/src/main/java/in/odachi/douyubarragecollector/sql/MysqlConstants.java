package in.odachi.douyubarragecollector.sql;

/**
 * Mysql SQL
 */
public class MysqlConstants {

    // 房间信息插入语句
    public static final String INSERT_TABLE_ROOM = "insert ignore into room (`room_id`,`cate_id`,`cate_name`,`room_name`," +
            "`owner_name`,`owner_weight`,`fans_num`,`avatar`,`date_time`) values(?,?,?,?,?,?,?,?,?) " +
            "on duplicate key update `room_name`=?, `owner_weight`=?, `fans_num`=?, `avatar`=?, `date_time`=?";

    // 房间信息插入语句
    public static final String INSERT_TABLE_GIFT = "insert ignore into gift (`id`,`name`,`type`,`pc`," +
            "`gx`,`desc`,`intro`,`mimg`,`himg`) values(?,?,?,?,?,?,?,?,?)";

    // 查询所有房间
    public static final String SELECT_TABLE_ROOM = "select * from room where `room_id`=?";

    // 查询所有礼物
    public static final String SELECT_TABLE_GIFT = "select * from gift where `id`=?";
}
