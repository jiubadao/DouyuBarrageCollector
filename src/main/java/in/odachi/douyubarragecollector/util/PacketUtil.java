package in.odachi.douyubarragecollector.util;

import in.odachi.douyubarragecollector.constant.Constants;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 斗鱼弹幕协议信息封装类
 */
public class PacketUtil {

    private static final Logger logger = Logger.getLogger(PacketUtil.class);

    // 数据包构造类
    private static class DyPacket {

        private final StringBuilder builder = new StringBuilder();

        /**
         * 返回弹幕协议格式化后的结果
         */
        private byte[] format() {
            // 数据包末尾必须以'\0'结尾
            String data = builder.append('\0').toString();
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
            try {
                byteArrayOutputStream.reset();
                // 加上消息头
                // 4 bytes packet length
                outputStream.write(FormatterUtil.toLH(data.length() + 8), 0, 4);
                // 4 bytes packet length
                outputStream.write(FormatterUtil.toLH(data.length() + 8), 0, 4);
                // 2 bytes message type
                outputStream.write(FormatterUtil.toLH(Constants.MESSAGE_TYPE_CLIENT), 0, 2);
                // 1 bytes encrypt
                outputStream.writeByte(0);
                // 1 bytes reserve
                outputStream.writeByte(0);
                // append data
                outputStream.writeBytes(data);
            } catch (RuntimeException | IOException e) {
                logger.error(LogUtil.printStackTrace(e));
            }
            return byteArrayOutputStream.toByteArray();
        }

        /**
         * 根据斗鱼弹幕协议进行相应的编码处理
         */
        private void addItem(String key, Object value) {
            builder.append(key.replaceAll("/", "@S").replaceAll("@", "@A"));
            builder.append("@=");
            if (value instanceof String) {
                builder.append(((String) value).replaceAll("/", "@S").replaceAll("@", "@A"));
            } else if (value instanceof Integer) {
                builder.append(value);
            }
            builder.append("/");
        }
    }

    /**
     * 生成登录请求数据包
     */
    public static byte[] getLoginRequestPacket(int roomId) {
        DyPacket packet = new DyPacket();
        packet.addItem("type", "loginreq");
        packet.addItem("roomid", roomId);
        return packet.format();
    }

    /**
     * 生成加入弹幕分组池数据包
     */
    public static byte[] getJoinGroupPacket(int roomId, int groupId) {
        DyPacket packet = new DyPacket();
        packet.addItem("type", "joingroup");
        packet.addItem("rid", roomId);
        packet.addItem("gid", groupId);
        return packet.format();
    }

    /**
     * 生成心跳协议数据包
     */
    public static byte[] getKeepAlivePacket() {
        DyPacket packet = new DyPacket();
        packet.addItem("type", "keeplive");
        packet.addItem("tick", (int) (System.currentTimeMillis() / 1000));
        return packet.format();
    }

    /**
     * 将弹幕按协议解析成Map结构
     */
    public static Map<String, Object> parseBarrageMsgToMap(String data) {
        Map<String, Object> rtnMsg = new HashMap<>();
        // 处理数据字符串末尾的'/0字符'
        data = StringUtils.substringBeforeLast(data, "/");
        String[] buff = data.split("/");
        for (String tmp : buff) {
            String key = StringUtils.substringBefore(tmp, "@=");
            Object value = StringUtils.substringAfter(tmp, "@=");

            if (StringUtils.contains((String) value, "@A")) {
                value = ((String) value).replaceAll("@S", "/").replaceAll("@A", "@");
                // 如果value值中包含子序列化值，则进行递归分析
                value = parseBarrageMsgToMap((String) value);
            }
            rtnMsg.put(key, value);
        }
        return rtnMsg;
    }
}
