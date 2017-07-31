package in.odachi.douyubarragecollector.intf;

import in.odachi.douyubarragecollector.constant.Constants;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 消息实体
 */
public class Message implements Serializable {

    private static final long serialVersionUID = 999900001234567890L;

    private String message;

    private String dateTime;

    public Message() {
    }

    public Message(String dataBodyStr) {
        this.message = dataBodyStr;
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(Constants.DATETIME_PATTERN);
        this.dateTime = LocalDateTime.now().format(dateTimeFormatter);
    }

    public Message(String dataBodyStr, String now) {
        this.message = dataBodyStr;
        this.dateTime = now;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    @Override
    public String toString() {
        return "rtime@=" + dateTime + "/" + message;
    }
}
