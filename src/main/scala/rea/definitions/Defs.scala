package rea.definitions

import java.net.URI
import java.util.{ BitSet => JBitSet, Set => JSet }
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.collection.mutable.BitSet
import scala.math.{log, min, max}
import scala.reflect.{ClassTag, classTag}
import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import com.google.common.net.InternetDomainName
import com.google.gson.Gson
import com.google.gson.annotations.SerializedName
import com.google.common.collect.Sets
import rea.cover.{ResultScorer, DistanceMeasure}
import rea.analysis.analyze
import rea.knowledge.Domains
import rea.util.analyzedJavaSet
import rea.util.javaSet
import rea.util.splitURL
import de.tudresden.matchtools.similarities._
import webreduce.typing.DataType

object JSONSerializable {
  val gson = new Gson
  def fromJson[C: ClassTag](t: Class[C], s: String): C = {
    JSONSerializable.gson.fromJson(s, classTag[C].runtimeClass)
  }
}

trait JSONSerializable {
  def toJson = {
    JSONSerializable.gson.toJson(this)
  }
}

class REARequest(val concept: Array[String], val entities: Array[String], val attribute: Array[String],
    val numResults: Int, val entityTables: Boolean = true) extends JSONSerializable {
  @transient
  val termSet = analyzedJavaSet(
    entities.flatMap(_.split(" ")), attribute.flatMap(_.split(" ")), concept)
}

class REAContinueRequest(val handle: Int, val numResults: Int) extends JSONSerializable

class REATermFrequencyRequest(val term: String) extends JSONSerializable

class READatasetRequest(val url: String) extends JSONSerializable

case class MatchInfo(attCol: Int, attRow:Int, attributeContext: String)

case class DrilledDataset(
  @transient var req:REARequest,
  dataset: Dataset,
  values: Array[DrilledValue],
  scores: DrillScores,
  matchInfo: MatchInfo) {

  def score = scores.score // should be the same for all drilledvalues
  def inherentScore = scores.inherentScore // should be the same for all drilledvalues
  @transient
  lazy val stats = {
    val s = new SummaryStatistics()
    values.foreach(v => s.addValue(v.value))
    s
  }

  @transient
  lazy val coveredEntities = BitSet.empty ++ values.map(_.forIndex)

  @transient
  lazy val matchedAttribute = {
    val attr = dataset.relation(matchInfo.attCol)(matchInfo.attRow)
    if (matchInfo.attributeContext != null)
      analyze(attr + " " + matchInfo.attributeContext)
    else
      analyze(attr)
  }

  @transient lazy val urlTags = Sets.intersection(dataset.urlTermSet, DrilledDataset.possibleTags)
  @transient lazy val titleTags = Sets.intersection(dataset.titleTermSet, DrilledDataset.possibleTags)
  @transient lazy val attributeTags = Sets.intersection(javaSet(normalizedMatchedAttribute.split(" ")), DrilledDataset.possibleTags)
  @transient lazy val tags = javaSet(Sets.union(  Sets.union(urlTags, titleTags), attributeTags).map(t => DrilledDataset.tagEquivalencies.getOrElse(t, t)))

  @transient
  lazy val normalizedMatchedAttribute = {
    val regEx = req.attribute.map(analyze).mkString("|")
    matchedAttribute.replaceAll(regEx, "<ATTR>")
  }

  def niceString() = {
    "drDs: " + dataset.shortString + " " + dataset.url +" " +values.map(_.forIndex).mkString("(",",",")") + scores
  }
  override def toString() = {
    "->drDs: " + dataset.shortString + " " + dataset.url + " " + values.map(_.forIndex).mkString("(",",",")") + scores
  }
  def toString(entities:Array[String]) = {
    val sb = new StringBuilder
    sb ++= s"From ${Console.RED}${dataset.title}${Console.RESET} (${dataset.tableNum}) ${Console.MAGENTA}${inherentScore}${Console.RESET} with matchedAttr ${Console.GREEN}${matchedAttribute}${Console.RESET}: ${dataset.domain} ${dataset.url}\n"
    for (vv <- values) {
      sb ++= "    "
      sb ++= vv.toString(entities)
      sb ++= "\n"
    }
    sb.toString
  }
}

object DrilledDataset {

  val possibleTags = javaSet(Seq(
        "rank", "growth", "change", "mil", "bil", "million", "billion", "meter", "ft", "feet", "km", "mile", "m", "b", "millions", "billions", "sq", "square"
      ) ++ (1990 until 2020).map(_.toString))
  val tagEquivalencies = Map(
    "change" -> "growth",
    "mil" -> "million",
    "bil" -> "billion",
    "meters" -> "meter",
    "ft" -> "feet",
    "km" -> "kilometer",
    "sq" -> "square",
    "kilometers" -> "kilometer",
    "miles" -> "mile",
    "m" -> "million",
    "b" -> "billion",
    "millions" -> "million",
    "billions" -> "billion")

  def normalize(results: Seq[DrilledDataset]) = {
    val maxTermSim = max(results.map(_.scores.termSim).max.toDouble, 0.001)
    val maxTitleSim = max(results.map(_.scores.titleSim).max.toDouble, 0.001)
    val domainPopularityScores = results.map(_.scores.domainPopularity)
    val dNorm = domainPopularityNormalizer(domainPopularityScores.min, domainPopularityScores.max)

    val preNormalized = results.map(_ match {
      case DrilledDataset(req, dataset, v, DrillScores(a, e, c, tr, t, ca, cb, dp, sc, inhSc), mi) =>
        DrilledDataset(req, dataset, v, DrillScores.create(
          a, e, c, tr / maxTermSim, t / maxTitleSim, ca, cb, dNorm(dp)), mi)
    })

    val scores = preNormalized.map(_.scores.score)
    val inherentScores = preNormalized.map(_.scores.inherentScore)

    val sNorm = scoreNormalizer(scores.min, scores.max)
    val isNorm = scoreNormalizer(inherentScores.min, inherentScores.max)

    val result = preNormalized.map(_ match {
      case DrilledDataset(req, dataset, v, DrillScores(a, e, c, tr, t, ca, cb, dp, sc, inhSc), mi) =>
        DrilledDataset(req, dataset, v, DrillScores(
          a, e, c, tr, t, ca, cb, dp, sNorm(sc), isNorm(inhSc)), mi)
    })
    result
  }

  def domainPopularityNormalizer(domainMinRank: Double, domainMaxRank: Double) = {
    val minR = log(domainMinRank)
    val maxR = log(domainMaxRank) + 0.0000001
    (x: Double) => (1.0 - (log(x) - minR) / (maxR - minR))
  }

  def scoreNormalizer(minScore: Double, maxScore: Double) = {
    val minR = minScore
    val maxR = maxScore + 0.0000001
    // println(s"maxScore: $maxScore minScore: $minScore")
    (x: Double) => (x - minR) / (maxR - minR)
  }
}

class Cover(val datasets: Seq[DrilledDataset], val datasetIndices:Seq[Int], val picks: Seq[Seq[DrilledValue]]) {

  val valSet = javaSet(picks.flatten)
  val valByEntity: Map[Int, DrilledValue] = picks.flatten.map(v => (v.forIndex,v)).toMap
  // val valIdSet = {
  //   val result = new JBitSet()
  //   picks.flatten.foreach { v => result.set(v.globalId) }
  //   result
  // }

  override def equals(other:Any) = {
    // other.isInstanceOf[Cover] && (datasetIndices == other.asInstanceOf[Cover].datasetIndices)
    other.isInstanceOf[Cover] && (valSet == other.asInstanceOf[Cover].valSet)
  }
  override def hashCode() = {
    val prime = 31
    var result = 1
    result = prime * result + valSet.hashCode()
    result
  }


  lazy val signature = datasets.map(_.dataset.signature).mkString("|")

  def minSources() = {
    val numDs = datasets.size.toDouble
    val numValues = this.picks.flatten.size.toDouble
    if (numDs == 1)
      1.0
    else
      1.0 - (numDs / numValues)
  }

  def coverQuality(numEntities:Int) = {
    minSources * coverage(numEntities)
  }

  def consistency(cMat:Array[Array[Double]]):Double = {
    var cnt = 0
    var sum = 0.0
    // val size = this.datasetIndices.size
    for (i <- this.datasetIndices.indices; j <- this.datasetIndices.indices; if j > i) {
        val dsi = this.datasetIndices(i)
        val dsj = this.datasetIndices(j)
        val numPicksi = this.picks(i).size
        val numPicksj = this.picks(j).size
        sum += cMat(dsi)(dsj) * numPicksi * numPicksj
        cnt += numPicksj * numPicksi
        // println(s"i:$i j:$j dsi:$dsi dsj $dsj numPicksi:$numPicksi, numPicksj:$numPicksj cMatEntry:${cMat(dsi)(dsj)} RESULT: ${cMat(dsi)(dsj) * numPicksi * numPicksj} CNT: ${numPicksj * numPicksi}")

    }
    if (cnt == 0)
        1.0
    else
        sum / cnt.toDouble
  }

  // def inherentScore():Double =
  //   this.datasets.map(_.inherentScore).sum / this.datasets.size

  def inherentScore():Double = {
    var cnt = 0
    var sum = 0.0
    for (i <- this.datasetIndices.indices) {
        val ds = this.datasets(i)
        val numPicks = this.picks(i).size
        sum += ds.inherentScore * numPicks
        cnt += numPicks
    }
    if (cnt == 0)
        1.0
    else
        sum / cnt.toDouble
  }

  def coverage(numEntities:Int) =
    this.picks.flatten.size.toDouble / numEntities

  def complete(numEntities: Int) = {
    coverage(numEntities) == 1.0
  }

  def toString(entities: Array[String]): String = {
    val sb = new mutable.StringBuilder()
    for ((ds, dsi) <- datasets.zipWithIndex) {
      sb ++= s"From ${Console.RED}${ds.dataset.title}${Console.RESET} (${ds.dataset.tableNum}) ${Console.MAGENTA}${ds.inherentScore}${Console.RESET} with matchedAttr ${Console.GREEN}${ds.matchedAttribute}${Console.RESET}: ${ds.dataset.domain} ${ds.dataset.url}\n"
      val v = picks(dsi)
      for (vv <- v) {
        sb ++= "    "
        sb ++= vv.toString(entities)
        sb ++= "\n"
      }
    }
    sb.toString()
  }

  override def toString(): String = {
    val sb = new mutable.StringBuilder()
    for (ds <- datasets) {
      sb ++= s"From ${Console.RED}${ds.dataset.title}${Console.RESET} (${ds.dataset.tableNum}) ${Console.MAGENTA}${ds.inherentScore}${Console.RESET} with matchedAttr ${Console.GREEN}${ds.matchedAttribute}${Console.RESET}: ${ds.dataset.url}\n"
    }
    sb.toString()
  }
}

object Cover {

  def diversity(xs: Seq[Cover], measure: DistanceMeasure) =
    if (xs.size < 2)
      0.0
    else
      (xs.combinations(2).map(
          cs => measure.distance(cs(0), cs(1))
      ).sum / ((xs.size * (xs.size-1)) / 2.0))

  def score(xs: Seq[Cover], scorer: ResultScorer): Double =
    xs.map(scorer.score(_)).sum / xs.size.toDouble

  def inherentScore(xs: Seq[Cover]):Double =
      xs.map(_.inherentScore).sum / xs.size.toDouble

  def minSources(xs: Seq[Cover]):Double =
      xs.map(_.minSources).sum / xs.size.toDouble

  def coverage(xs: Seq[Cover], numEntities:Int):Double =
      xs.map(_.coverage(numEntities)).sum / xs.size.toDouble

  def coverQuality(xs: Seq[Cover], numEntities:Int):Double =
      xs.map(_.coverQuality(numEntities)).sum / xs.size.toDouble

  def consistency(xs: Seq[Cover], scorer: ResultScorer):Double =
      xs.map(_.consistency(scorer.cMat)).sum / xs.size.toDouble
}

case class DrilledValue(
  forIndex: Int,
  value: Double,
  @transient var dataset: Dataset,
  eColIdx: Int,
  eRowIdx: Int,
  colIdx: Int,
  rowIdx: Int,
  sim: Double,
  globalId: Int) {
  def prettyPrint(e: Seq[String]) = {
    println(toString(e))
  }
  def getValue() = value
  def toString(e: Seq[String]) = s"Value for: ${Console.YELLOW}${e(forIndex)}${Console.RESET}, value: $value, eCol: $eColIdx, eRow: $eRowIdx, matched: ${Console.YELLOW}${dataset.relation(eColIdx)(eRowIdx)}${Console.RESET}, vCol: $colIdx, vRow: $rowIdx, oValue: ${dataset.relation(colIdx)(rowIdx)}, sim: $sim"
  def shortString() = s"Value for $forIndex from ${dataset.shortString}"
}

case class DrillScores(
  val attSim: Double,
  val entitySim: Double,
  val conceptSim: Double,
  val termSim: Double,
  val titleSim: Double,
  val coverageA: Double,
  val coverageB: Double,
  val domainPopularity: Double,
  val score:Double,
  val inherentScore:Double) {

  override def toString = s"""DrillScores A:$attSim E:$entitySim C:$conceptSim TR:$termSim TI:$titleSim COVA:$coverageA COVB:$coverageB DOMP:$domainPopularity SCORE:$score INH_SCORE:$inherentScore"""
}

object DrillScores {
  def create(attSim: Double, entitySim: Double, conceptSim: Double, termSim: Double, titleSim: Double, coverageA: Double, coverageB: Double, domainPopularity: Double) =
    DrillScores(attSim, entitySim, conceptSim, termSim, titleSim, coverageA, coverageB, domainPopularity,
      (attSim + entitySim * 0.9 + conceptSim * 0.01 + termSim * 0.01 + titleSim * 0.01 + coverageA + coverageB * 0.01  + domainPopularity * 0.5),
      // temporarily changing inherentScore to mean the same as score
      (attSim + entitySim * 0.9 + conceptSim * 0.01 + termSim * 0.01 + titleSim * 0.01 + coverageA + coverageB * 0.01 + domainPopularity * 0.5))
      // (attSim + entitySim * 0.9 + conceptSim * 0.01 + termSim * 0.01 + titleSim * 0.01 +             coverageB * 0.01 + domainPopularity * 0.5))
}

class Dataset extends JSONSerializable {

  var relation: Array[Array[String]] = null
  var id: String = null
  var url: String = null
  var hasHeader: Boolean = false

  var recordOfffset: Long = -1 // typo in original datasetst
  var recordOffset: Long = -1
  var recordEndOffset: Long = -1
  var tableNum: Int = -1
  var s3Link: String = null

  var indexId: Int = -1

  override def equals(that: Any): Boolean = that match {
    case other: Dataset => this.url == other.url && this.tableNum == other.tableNum
    case _ => false
  }
  override def hashCode() = {
    val prime = 31
    var result = 1
    result = prime * result + url.hashCode()
    result = prime * result + tableNum.hashCode()
    result
  }

  lazy val signature = this.title + ":" + this.tableNum

  @SerializedName("columnTypes") var raw_columnTypes: Array[String] = null
  @SerializedName("domain") var raw_domain: String = null
  @SerializedName("attributes") var raw_attributes: Array[String] = null
  @SerializedName("title") var raw_title: String = null
  @SerializedName("terms") var raw_terms: String = null
  @SerializedName("termSet") var raw_termSet: Array[String] = null
  @SerializedName("urlTermSet") var raw_urlTermSet: Array[String] = null
  @SerializedName("titleTermSet") var raw_titleTermSet: Array[String] = null

  /* serialized/deserialized until here, rest is transient and lazy, so that
  the class can be used both on the remoteIndexServer and the REA tools without changes */

  @transient lazy val colTypes =
    raw_columnTypes.map(DataType.byString(_))

  @transient lazy val domain =
    if (raw_domain == null)
      InternetDomainName.from(new URI(url).getHost).topPrivateDomain.toString
    else
      raw_domain

  @transient lazy val domainInfo = { Domains.query(domain) }

  @transient lazy val attributes =
    if (raw_attributes == null)
      for (c <- relation) yield c(0)
    else
      raw_attributes

  @transient lazy val termSet = javaSet(
    if (raw_termSet == null)
      raw_terms.split(" ").map(analyze)
    else
      raw_termSet)

  @transient lazy val urlTermSet = javaSet(
    if (raw_urlTermSet == null)
      splitURL(url).map(analyze).filter(_ != "")
    else
      raw_urlTermSet)

  @transient lazy val titleTermSet = javaSet(
    if (raw_titleTermSet == null)
      title.split(" ").map(analyze).filter(_ != "")
    else
      raw_titleTermSet)

  @transient lazy val title = raw_title match {
    case s: String => s
    case null => url
  }

  def prettyColumns(cols: Seq[Int], n: Int = Integer.MAX_VALUE) {
    for (r <- relation(0).indices.take(n)) {
      for (c <- cols) {
        val s = relation(c)(r)
        if (s.length() >= 30) {
          print(s.substring(0, 30) + "|")
        } else {
          val d = " " * (30 - s.length())
          print(s + d + "|")
        }
      }
      println()
    }
  }

  def prettyTable(n: Int = Integer.MAX_VALUE, mark: Option[(Int, Int)] = None) = {
    def markUp(s: String, c: Int, r: Int) =
      if (Some( (c, r) ) == mark) Console.RED + s + Console.RESET
      else s

    val sb = new StringBuilder()
    /* print header */
    for (c <- relation.indices) {
      val s = relation(c)(0)
      if (s.length() >= 30) {
        sb ++= markUp(s.substring(0, 30), c, 0) + "|"
      } else {
        val d = " " * (30 - s.length())
        sb ++= markUp(s, c, 0) + d + "|"
      }
    }
    sb ++= "\n"
    sb ++= ("-" * (30 * relation.size + 1))
    sb ++= "\n"

    for (r <- relation(0).indices.drop(1).take(n)) {
      for (c <- relation.indices) {
        val s = relation(c)(r)
        if (s.length() >= 30) {
          sb ++= markUp(s.substring(0, 30), c, r) + "|"
        } else {
          val d = " " * (30 - s.length())
          sb ++= markUp(s, c, r) + d + "|"
        }
      }
      sb ++= "\n"
    }
    sb.toString
  }

  override def toString = s"""Dataset: $title from $url at $tableNum"""
  def shortString = s"ds: ${title.substring(0, min(title.size, 40))} ${domain}"
}
