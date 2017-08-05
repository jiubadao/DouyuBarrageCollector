package in.odachi.douyubarragecollector.master.tokenizer;

import com.hankcs.hanlp.corpus.tag.Nature;
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary;
import com.hankcs.hanlp.seg.CRF.CRFSegment;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.List;

/**
 * 分词服务
 */
public class Tokenizer {

    private static Segment segment = new CRFSegment();

    static {
        segment.enablePartOfSpeechTagging(true);
    }

    /**
     * 对弹幕进行分词
     */
    public static void segment(Integer roomId, String sentence) {
        List<Term> termList = segment.seg(sentence);
        termList.forEach(term -> {
            if (StringUtils.isNotBlank(term.word) && !term.nature.startsWith("w") &&
                    CoreStopWordDictionary.shouldInclude(term) &&
                    (term.nature != Nature.nz || NumberUtils.isNumber(term.word))) {
                TokenizerCollection.INSTANCE.getTokenizerMap(roomId).compute(term.word, (k, v) -> v == null ? 1 : v + 1);
            }
        });
    }
}