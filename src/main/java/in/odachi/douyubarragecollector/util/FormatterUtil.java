package in.odachi.douyubarragecollector.util;

/**
 * 数据库相关工具类
 */
public class FormatterUtil {

    public static byte[] toLH(int n) {
        final byte[] b = new byte[4];
        b[0] = (byte) (n & 0xff);
        b[1] = (byte) (n >> 8 & 0xff);
        b[2] = (byte) (n >> 16 & 0xff);
        b[3] = (byte) (n >> 24 & 0xff);
        return b;
    }

    public static int LHtoI(byte[] b) {
        return b[0] & 0xFF | (b[1] & 0xFF) << 8
                | (b[2] & 0xFF) << 16
                | (b[3] & 0xFF) << 24;
    }

    public static Integer parseInt(Object o) {
        if (o instanceof Integer) {
            return (Integer) o;
        }
        try {
            return Integer.parseInt((String) o);
        } catch (RuntimeException e) {
            return -1;
        }
    }
}
