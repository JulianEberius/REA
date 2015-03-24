package rea

import scala.math.min
import java.util.regex.Pattern
import java.net.URL
import java.net.URI
import java.net.MalformedURLException
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.analysis.standard.ClassicAnalyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import webreduce.cleaning.CustomAnalyzer
import java.io.StringReader
import org.apache.lucene.util.Version
import scala.collection.mutable
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute

package object analysis {

    val reaAnalyzer = new CustomAnalyzer();

    def clean(s: String) = s.replaceAll("[^A-Za-z ]", "").replaceAll("[ ]{2,}", " ")

    private def _analyzeWith(analyzer: Analyzer)(s: String):Iterable[String] = {
        val result = new mutable.ArrayBuffer[String]()
        val stream = analyzer.tokenStream(null, new StringReader(s))
        stream.reset()
        while (stream.incrementToken())
            result += stream.getAttribute(classOf[CharTermAttribute]).toString
        stream.end()
        stream.close()
        result
    }
    def analyzeWith(analyzer: Analyzer)(s: String):String =
        _analyzeWith(analyzer)(s).mkString(" ")

    def analyzeToSetWith(analyzer: Analyzer)(s: String):Set[String] =
        _analyzeWith(analyzer)(s).toSet[String]

    val analyze = analyzeWith(reaAnalyzer)_

    val analyzeToSet = analyzeToSetWith(reaAnalyzer)_

}
