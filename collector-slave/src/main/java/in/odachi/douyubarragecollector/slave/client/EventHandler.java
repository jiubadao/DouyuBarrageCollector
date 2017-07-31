package in.odachi.douyubarragecollector.slave.client;

import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.constant.RedisKeys;
import in.odachi.douyubarragecollector.intf.Message;
import in.odachi.douyubarragecollector.util.FormatterUtil;
import in.odachi.douyubarragecollector.util.PacketUtil;
import in.odachi.douyubarragecollector.util.RedisUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * 消息收发
 */
class EventHandler {

    private static final Logger logger = Logger.getLogger(EventHandler.class);

    private static final int HEADER_SIZE = 4;

    private final BlockingQueue<Message> queue;

    private final Integer roomId;

    private final Map.Entry<String, Integer> channelEntry;

    private ByteBuffer inputBuf = null;

    // 是否已经发送登录消息
    private boolean isLogin = false;

    // 是否已经登录成功
    private boolean isLoginSuccess = false;

    // 连续接受到海量RSS消息问题
    private static final int MAX_NUMBER_OF_RSS_MSG = 10;

    // 连续出现RSS消息的数量，该问题会导致服务器CPU超载
    private int continuousRssCount = 0;

    private final ReactorManager reactorManager;

    /**
     * 客户端初始化
     */
    EventHandler(ReactorManager reactorManager, Integer roomId, Map.Entry<String, Integer> channelEntry) {
        this.reactorManager = reactorManager;
        this.roomId = roomId;
        this.queue = RedisUtil.client.getBlockingQueue(RedisKeys.DOUYU_SYSTEM_MESSAGE_QUEUE);
        this.channelEntry = channelEntry;
    }

    /**
     * 处理连接事件
     */
    void connect(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        if (!channel.finishConnect()) {
            logger.error("Channel connect FAILED: " + toString());
            reactorManager.closeChannel(key, false);
            return;
        }
        logger.trace("Channel connect SUCCESS: " + toString());
    }

    /**
     * 处理读事件
     * 斗鱼弹幕服务器采用TCP长连接通信，会频繁出现粘包或断包现象。
     * 根据协议，每个数据包会在首8个字节发送消息长度，并且以'\0'结尾（但是没提消息体里的'\0'是如何转义的）。
     * 这里选择根据消息长度采用ByteBuffer解决粘包问题。
     * 先读取头部信息，转换成消息长度，再读取相应长度的报文。
     * 同时斗鱼服务端疑似限制同IP最大连接数为200，我们这里监听channel.read()返回值用于判断连接是否被强制关闭了。
     */
    void read(SelectionKey key) throws IOException, InterruptedException {
        SocketChannel channel = (SocketChannel) key.channel();
        // inputBuf为空，表示开始读取一个新的消息包
        if (inputBuf == null || inputBuf.capacity() == HEADER_SIZE) {
            // 先读取头部信息，判断消息长度
            if (inputBuf == null) {
                inputBuf = ByteBuffer.allocate(HEADER_SIZE);
            }
            // if the peer closes the connection, OP_READ will fire and a read will return -1.
            if (channel.read(inputBuf) == -1) {
                logger.error("Channel is closed by the peer in step 1: " + toString());
                reactorManager.closeChannel(key, true);
                return;
            }
            // 没有将头部4字节读取完，等待继续读取
            if (inputBuf.hasRemaining()) {
                return;
            }

            // 包头部分已经接受完毕
            final int count = FormatterUtil.LHtoI(inputBuf.array());
            // 消息体长度应不小于8（此处可以不判断，但防止网络抖动。。）
            if (count <= HEADER_SIZE + 4) {
                logger.error("Packet headerBuf got count byte: " + count + ", channel is closing: " + toString());
                reactorManager.closeChannel(key, true);
            } else {
                // 读count长度的消息
                inputBuf = ByteBuffer.allocate(count);
            }
            return;
        }

        // 尝试读取数据区域
        if (channel.read(inputBuf) == -1) {
            logger.error("Channel is closed by the peer in step 2: " + toString());
            reactorManager.closeChannel(key, true);
            return;
        }
        // 数据还没有填充满，继续接受数据
        if (inputBuf.hasRemaining()) {
            return;
        }
        // 处理消息
        processData(key);
    }

    /**
     * 处理消息
     */
    private void processData(SelectionKey key) throws IOException, InterruptedException {
        // 消息体长度要剪掉8个字节，4个字节是消息长度，2字节小端整数表示消息类型，1个字节加密字段，1个字节保留字段。
        // 这里不二次校验消息长度是否相同，因为即使校验出不相同也没啥办法。。
        final int dataBodyLen = inputBuf.capacity() - HEADER_SIZE - 4;
        final byte[] dataBody = new byte[dataBodyLen];

        // 消息体跳过前8个字节
        System.arraycopy(inputBuf.array(), 8, dataBody, 0, dataBodyLen);
        String dataBodyStr = new String(dataBody);

        // 如果还未登陆成功，先解析登陆返回消息
        if (!isLoginSuccess) {
            if (!dataBodyStr.startsWith("type@=loginres/")) {
                logger.error("Login FAILED packet received, channel is closing: " + toString());
                reactorManager.closeChannel(key, false);
                return;
            }
            logger.debug("Login SUCCESS packet received: " + toString());
            isLoginSuccess = true;
            return;
        }

        // 房间开关播提醒
        if (dataBodyStr.startsWith("type@=rss/")) {
            // 如果是rss消息，计数器+1
            continuousRssCount++;
            // 如果连续收到超过MAX_NUMBER_OF_RSS_MSG个rss消息，则认为出现bug，关闭该链接
            if (continuousRssCount >= MAX_NUMBER_OF_RSS_MSG) {
                logger.warn("Received too many RSS packet in a row, channel is closing: " + toString());
                reactorManager.closeChannel(key, true);
                return;
            }

            int index = dataBodyStr.indexOf("ss@=");
            if (index < 0) {
                logger.debug("RSS packet received, but ss@= is not found, ignore: " + dataBodyStr);
                return;
            }

            char status = dataBodyStr.charAt(index + 4);
            logger.debug("RSS packet received, status: " + status + ", " + toString());
            if ('0' == status) {
                // 主播关播，退出
                reactorManager.closeChannel(key, false);
            }
            return;
        } else {
            // 如果接下来的消息不是rss消息，则计数器归零
            continuousRssCount = 0;
        }

        // 解析其他信息
        boolean success = queue.offer(new Message(dataBodyStr));
        if (success) {
            reactorManager.incrementProcessedTotalCount();
        } else {
            logger.error("Put message into queue FAILED: " + dataBodyStr);
        }
        // 数据已经接受完，继续接受新数据
        inputBuf = null;
    }

    /**
     * 处理写事件
     * 发送登陆消息、注册消息和心跳保持消息，每次只发送一个类型消息
     */
    void write(SelectionKey key) throws IOException {
        if (!isLogin) {
            loginRoom(key);
            joinGroup(key);
        } else {
            keepAlive(key);
        }
    }

    /**
     * 登录房间
     * 如果发送登陆消息失败则关闭连接，但不立即重建，等待下次抓取到该房间再正常处理
     */
    private void loginRoom(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        final byte[] loginRequestData = PacketUtil.getLoginRequestPacket(roomId);
        ByteBuffer writeBuf = ByteBuffer.allocate(loginRequestData.length);
        writeBuf.put(loginRequestData);
        // 将缓冲区各标志复位,因为向里面put了数据标志被改变要想从中读取数据发向服务器,就要复位
        writeBuf.flip();

        if (channel.write(writeBuf) <= 0) {
            // 登陆房间失败关闭连接
            logger.error("Send login request FAILED: " + toString());
            reactorManager.closeChannel(key, false);
            return;
        } else {
            logger.trace("Send login request SUCCESS: " + toString());
        }
        isLogin = true;
    }

    /**
     * 没有加入分组先发送加入分组消息
     */
    private void joinGroup(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        final byte[] joinGroupRequest = PacketUtil.getJoinGroupPacket(roomId, Constants.GROUP_ID);
        ByteBuffer writeBuf = ByteBuffer.allocate(joinGroupRequest.length);
        writeBuf.put(joinGroupRequest);
        writeBuf.flip();

        if (channel.write(writeBuf) <= 0) {
            // 加入分组失败关闭连接
            logger.error("Send join group request FAILED: " + toString());
            reactorManager.closeChannel(key, false);
        } else {
            logger.trace("Send join group request SUCCESS: " + toString());
        }
    }

    /**
     * 发送心跳信息
     */
    private void keepAlive(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        final byte[] keepAliveRequest = PacketUtil.getKeepAlivePacket();

        ByteBuffer writeBuf = ByteBuffer.allocate(keepAliveRequest.length);
        writeBuf.put(keepAliveRequest);
        writeBuf.flip();

        if (channel.write(writeBuf) <= 0) {
            // 发送失败关闭链接
            logger.error("Send keep alive request FAILED, channel is closing: " + toString());
            reactorManager.closeChannel(key, false);
        } else {
            logger.trace("Send keep alive request SUCCESS: " + toString());
        }
    }

    // Getter and setter
    public Integer getRoomId() {
        return roomId;
    }

    public Map.Entry<String, Integer> getChannelEntry() {
        return channelEntry;
    }

    public String toString() {
        return roomId + " (" + channelEntry.getKey() + ":" + channelEntry.getValue() + ")";
    }
}
