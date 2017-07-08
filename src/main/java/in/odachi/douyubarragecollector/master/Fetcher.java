package in.odachi.douyubarragecollector.master;

import com.jcabi.aspects.RetryOnFailure;
import in.odachi.douyubarragecollector.constant.Constants;
import in.odachi.douyubarragecollector.constant.LocalFileProperties;
import in.odachi.douyubarragecollector.util.FormatterUtil;
import in.odachi.douyubarragecollector.util.HttpUtil;
import in.odachi.douyubarragecollector.util.LogUtil;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 获取所有房间信息
 */
public class Fetcher {

    private static final Logger logger = Logger.getLogger(Fetcher.class);

    /**
     * 获取配置文件指定的房间
     */
    public static List<Map<String, Object>> fetchRoom() {
        List<Map<String, Object>> arrayList = new ArrayList<>();
        arrayList.addAll(fetchTopRoom());
        arrayList.addAll(fetchSpecifiedRoom());
        return arrayList;
    }

    /**
     * 抓取分类下排名靠前的房间
     * 这些房间必然是在直播状态，无需再次判断room_status
     */
    private static List<Map<String, Object>> fetchTopRoom() {
        Set<String> dirNameSet = LocalFileProperties.getDirNames();
        List<Map.Entry<Integer, Double>> entryList = new ArrayList<>(1024);
        List<Callable<String>> pageTasks = new ArrayList<>();
        // 转化成有序列表
        List<String> dirList = new ArrayList<>(dirNameSet);

        // 遍历所配的目录
        dirList.forEach(dir -> {
            String listUrl = Constants.HTTP_DIRECTORY_GAME + dir;
            pageTasks.add(new HttpUtil(listUrl));
        });

        List<Future<String>> futureList = ExecutorPool.invokeAll(pageTasks);
        int index = 0;
        for (Future<String> future : futureList) {
            // 先抓取页面解析页数
            String pattern = "\\{[\\s]*count:\\s\"(\\d+)\"[,\\s]*current:\\s\"\\d+\"[\\s]*}";
            Pattern r = Pattern.compile(pattern);
            Matcher m = null;
            try {
                m = r.matcher(future.get());
            } catch (InterruptedException | ExecutionException e) {
                logger.warn(e);
            }

            int pageSize = 1;
            if (m != null && m.find()) {
                try {
                    pageSize = FormatterUtil.parseInt(m.group(1));
                } catch (RuntimeException e) {
                    logger.warn(LogUtil.printStackTrace(e));
                }
            }

            String game = dirList.get(index++);
            String listUrl = Constants.HTTP_DIRECTORY_GAME + game;

            // 遍历所有页面，抓取所有在线房间
            for (int page = 1; page <= pageSize; page++) {
                Map<String, String> param = new HashMap<>(2);
                param.put("page", String.valueOf(page));
                param.put("isAjax", "1");

                Document doc;
                try {
                    doc = getDocument(listUrl, param);
                } catch (IOException e) {
                    logger.warn(e);
                    continue;
                }

                Elements lii = doc.getElementsByTag("li");
                lii.forEach(li -> {
                    Integer roomId = FormatterUtil.parseInt(li.attr("data-rid"));
                    Elements elements = li.getElementsByClass("dy-num");
                    String popularityStr = elements.get(0).text();
                    double popularity;
                    if (popularityStr.contains("万")) {
                        popularityStr = popularityStr.replaceAll("万", "");
                        popularity = Double.parseDouble(popularityStr) * 10000;
                    } else {
                        popularity = FormatterUtil.parseInt(popularityStr);
                    }
                    entryList.add(new AbstractMap.SimpleImmutableEntry<>(roomId, popularity));
                });
            }
        }
        logger.debug("Rooms in all directories: " + entryList.size());
        return getRoomMaps(entryList);
    }

    /**
     * 先排序取出一定数量房间，再获取这些房间详情
     */
    private static List<Map<String, Object>> getRoomMaps(List<Map.Entry<Integer, Double>> entryList) {
        // 按人气排序，取前一定数量的房间
        entryList.sort((o1, o2) -> (int) (o2.getValue() - o1.getValue()));
        Set<Integer> duplicate = new HashSet<>(1024);
        int popularityMin = LocalFileProperties.getPopularityMin();
        int subIndex = entryList.size() >= popularityMin ? popularityMin : entryList.size();
        entryList.subList(0, subIndex).forEach(entry -> duplicate.add(entry.getKey()));

        // 从缓存中取关注数量
        List<Callable<Map.Entry<Integer, Integer>>> tasks = new ArrayList<>();
        entryList.forEach(entry -> tasks.add(() -> LocalCache.getInstance().get(entry.getKey())));

        List<Future<Map.Entry<Integer, Integer>>> futureList = ExecutorPool.invokeAll(tasks);
        futureList.forEach(future -> {
            Map.Entry<Integer, Integer> entry;
            try {
                entry = future.get();
                if (entry.getValue() >= LocalFileProperties.getFansMin()) {
                    duplicate.add(entry.getKey());
                }
            } catch (InterruptedException | ExecutionException e) {
                logger.warn(e);
            }
        });
        logger.debug("Hot room size: " + duplicate.size());
        return Fetcher.fetchRoomInfo(duplicate);
    }

    /**
     * 提取页数
     * 失败重试一次
     */
    @RetryOnFailure(attempts = 2)
    private static Document getDocument(String listUrl, Map<String, String> param) throws IOException {
        logger.debug("Request URL: " + listUrl + ", " + param);
        return Jsoup.connect(listUrl).data(param).get();
    }

    /**
     * 抓取指定的房间列表
     * 仅抓取在直播状态的房间
     */
    private static List<Map<String, Object>> fetchSpecifiedRoom() {
        Set<Integer> roomIdSet = LocalFileProperties.getRoomIds();
        List<Map<String, Object>> specifiedRoomList = new ArrayList<>();
        fetchRoomInfo(roomIdSet).forEach(room -> {
            // 找出在线的房间进行监听
            if ("1".equals(room.get("room_status"))) {
                logger.debug("Specified room is online: " + room.get("room_id"));
                specifiedRoomList.add(room);
            } else {
                logger.debug("Specified room is NOT online: " + room.get("room_id"));
            }
        });
        logger.debug("Specified room size: " + specifiedRoomList.size());
        return specifiedRoomList;
    }

    /**
     * 通过第三方API获取房间详细信息
     * 会等待全部任务执行完一起返回结果
     */
    private static List<Map<String, Object>> fetchRoomInfo(final Collection<Integer> roomIdList) {
        List<Callable<String>> tasks = new ArrayList<>();
        roomIdList.forEach(roomId -> {
            String infoUrl = Constants.HTTP_API_ROOM + roomId;
            tasks.add(new HttpUtil(infoUrl));
        });

        List<Map<String, Object>> roomInfoList = new ArrayList<>();
        List<Future<String>> futureList = ExecutorPool.invokeAll(tasks);
        futureList.forEach(future -> {
            try {
                Map<String, Object> room = parseJson(future.get());
                if (room == null || "null".equals(String.valueOf(room.get("room_id")))) {
                    return;
                }
                roomInfoList.add(room);
            } catch (RuntimeException | ExecutionException | InterruptedException e) {
                logger.warn(e);
            }
        });
        return roomInfoList;
    }

    /**
     * 将请求返回转换为Json格式
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseJson(final String content) {
        Map<String, Object> jsonMap;
        ObjectMapper mapper = new ObjectMapper();
        try {
            jsonMap = mapper.readValue(content, new TypeReference<Map<String, Object>>() {
            });
        } catch (IOException e) {
            logger.warn("Json response convert to Map FAILED: " + content);
            return null;
        }

        int error = (int) jsonMap.get("error");
        if (error != 0) {
            logger.warn("Request FAILED: " + content);
            return null;
        }
        return (Map<String, Object>) jsonMap.get("data");
    }

    public static void main(String[] args) {
        System.out.println(fetchRoom().size());
    }
}
