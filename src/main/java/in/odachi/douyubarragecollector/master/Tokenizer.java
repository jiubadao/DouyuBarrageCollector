package in.odachi.douyubarragecollector.master;

import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.StandardTokenizer;
import in.odachi.douyubarragecollector.constant.Constants;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 分词服务
 */
public class Tokenizer {

    // 实时分词结果
    public static Map<String, Double> tokenizerMap = new ConcurrentHashMap<>(1024);

    static {
        try {
            // 自定义停留词
            Files.lines(Paths.get(Constants.STOP_FILE_NAME)).parallel().forEach(CoreStopWordDictionary::add);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 对弹幕进行分词
     */
    public static void segment(String sentence) {
        if (StringUtils.isNotBlank(sentence)) {
            List<Term> termList = StandardTokenizer.segment(sentence);
            CoreStopWordDictionary.apply(termList);
            termList.forEach(term -> {
                String word = term.word;
                tokenizerMap.compute(word, (k, v) -> v == null ? 1 : v + 1);
            });
        }
    }
}
