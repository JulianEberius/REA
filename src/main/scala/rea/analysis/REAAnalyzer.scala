package rea.analysis

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.core.StopAnalyzer
import org.apache.lucene.analysis.core.LowerCaseFilter
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter
import org.apache.lucene.analysis.standard.ClassicTokenizer
import org.apache.lucene.analysis.core.StopFilter
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents
import org.apache.lucene.analysis.TokenFilter
import org.apache.lucene.analysis.standard.ClassicFilter
import org.apache.lucene.util.Version
import java.io.Reader

class REAAnalyzer extends Analyzer {

    override def createComponents(fieldName:String, reader:Reader): TokenStreamComponents = {
        val src = new ClassicTokenizer(Version.LUCENE_45, reader);
        src.setMaxTokenLength(255);
        var filter:TokenFilter = new ClassicFilter(src);
        filter = new LowerCaseFilter(Version.LUCENE_45, filter);
        filter = new StopFilter(Version.LUCENE_45, filter, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
        filter = new ASCIIFoldingFilter(filter);
        return new TokenStreamComponents(src, filter) {
            override def setReader(reader:Reader) = {
                src.setMaxTokenLength(255);
                super.setReader(reader);
            }
        };
    }
}