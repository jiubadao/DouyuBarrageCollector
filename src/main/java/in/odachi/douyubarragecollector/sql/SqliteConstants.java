package in.odachi.douyubarragecollector.sql;

/**
 * Sqlite SQL
 */
public class SqliteConstants {
    // 弹幕消息表
    public static final String CREATE_TABLE_MSG = "create table if not exists MSG_#date (" +
            "id integer primary key," +
            "date_time text," +
            "rid integer," +
            "uid integer," +
            "nn text," +
            "txt text," +
            "cid text," +
            "level integer" +
            ")";

    // 赠送礼物消息表
    public static final String CREATE_TABLE_DGB = "create table if not exists DGB_#date (" +
            "id integer primary key," +
            "date_time text," +
            "rid integer," +
            "gfid integer," +
            "uid integer," +
            "nn text," +
            "level integer," +
            "hits integer" +
            ")";

    // 房间信息表
    public static final String CREATE_TABLE_ROOM = "create table if not exists ROOM (" +
            "room_id integer primary key unique," +
            "cate_id integer," +
            "cate_name text," +
            "room_name text," +
            "owner_name text," +
            "owner_weight text," +
            "fans_num integer," +
            "avatar text," +
            "date_time text" +
            ")";

    // 礼物信息表
    public static final String CREATE_TABLE_GIFT = "create table if not exists GIFT (" +
            "id text primary key unique," +
            "name text," +
            "type text," +
            "pc integer," +
            "gx integer," +
            "desc text," +
            "intro text," +
            "mimg text," +
            "himg text" +
            ")";

    // 弹幕消息插入语句
    public static final String INSERT_TABLE_MSG = "insert into msg_#date (rid,uid,nn,txt,cid,level,date_time) values(?,?,?,?,?,?,?)";

    // 礼物消息插入语句
    public static final String INSERT_TABLE_DGB = "insert into dgb_#date (rid,gfid,uid,nn,level,hits,date_time) values(?,?,?,?,?,?,?)";

    // 房间信息插入语句
    public static final String INSERT_TABLE_ROOM = "insert or ignore into room (room_id,cate_id,cate_name,room_name," +
            "owner_name,owner_weight,fans_num,avatar,date_time) values(?,?,?,?,?,?,?,?,?)";

    // 房间信息插入语句
    public static final String INSERT_TABLE_GIFT = "insert or ignore into gift (id,name,type,pc," +
            "gx,desc,intro,mimg,himg) values(?,?,?,?,?,?,?,?,?)";

    // 弹幕数量
    public static final String ANALYSIS_MSG_COUNT = "select t.rid, count(0) as c from msg_#date t group by t.rid";

    // 弹幕人次
    public static final String ANALYSIS_MSG_USER = "select t1.rid, count(0) as c from (select t.uid, t.rid from msg_#date t group by t.uid, t.rid) t1 group by t1.rid";

    // 礼物数量
    public static final String ANALYSIS_DGB_COUNT = "select t.rid, count(0) as c from dgb_#date t group by t.rid";

    // 礼物人次
    public static final String ANALYSIS_DGB_USER = "select t1.rid, count(0) as c from (select t.uid, t.rid from dgb_#date t group by t.uid, t.rid) t1 group by t1.rid";

    // 礼物价值
    public static final String ANALYSIS_DGB_PRICE = "select t.rid as rid, sum(t.pc * t.count * t.rate) as c\n" +
            "from (select t0.rid, t0.gfid, t0.count, g.name, g.type, g.pc,\n" +
            "case when g.type='2'\n" +
            "then 1\n" +
            "else 0.001\n" +
            "end as rate\n" +
            "from (select d.rid, d.gfid, count(0) as count from dgb_#date d group by d.rid, d.gfid) t0\n" +
            "left join gift g on g.id=t0.gfid) t\n" +
            "group by t.rid";

    // 删除表
    public static final String DROP_TABLE = "drop table if exists #name";

    // 查询所有房间
    public static final String SELECT_TABLE_ROOM = "select * from room";

    // 查询所有礼物
    public static final String SELECT_TABLE_GIFT = "select * from gift";
}
