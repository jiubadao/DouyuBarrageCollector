package in.odachi.douyubarragecollector.util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * 日志工具类
 */
public class LogUtil {
    public static String printStackTrace(Exception e) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        e.printStackTrace(printWriter);
        return stringWriter.toString();
    }
}
