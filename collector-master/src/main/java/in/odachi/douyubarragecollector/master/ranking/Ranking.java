package in.odachi.douyubarragecollector.master.ranking;

/**
 * 实时排名
 */
public class Ranking {

    /**
     * 增加一个弹幕计数
     */
    public static void incrementMessage(Integer roomId, Integer userId) {
        // 弹幕数量
        RankingCollection.INSTANCE.incrementMsgCount(roomId);
        // 弹幕人次
        RankingCollection.INSTANCE.incrementMsgUser(roomId, userId);
    }

    /**
     * 增加一个礼物计数
     */
    public static void incrementGift(Integer roomId, Integer userId, Integer giftId) {
        // 礼物数量
        RankingCollection.INSTANCE.incrementGiftCount(roomId);
        // 礼物人次
        RankingCollection.INSTANCE.incrementGiftUser(roomId, userId);
        // 礼物类型
        RankingCollection.INSTANCE.incrementGiftType(roomId, giftId);
    }
}