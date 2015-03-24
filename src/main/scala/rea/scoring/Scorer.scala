package rea.scoring

import scala.io.Source
import scala.math.{min, max, abs, log10}
import rea.definitions.Dataset
import rea.definitions.JSONSerializable
import rea.definitions.REARequest
import rea.definitions.DrilledValue
import rea.knowledge.Domains
import rea.Config
import rea.util.jaccard
import rea.util.{Predicate, NumericPredicate, NumericValuedPredicate}
import rea.analysis.analyze
import de.tudresden.matchtools.MatchTools
import org.apache.commons.math3.stat.descriptive.SummaryStatistics

object Scorer {

  val matchTools = MatchTools.DEFAULT

  def conceptSim(req: REARequest, ds: Dataset, cColIdx:Int) = {
    val attr = ds.attributes(cColIdx)
    val concepts = for (c <- req.concept)
      yield matchTools.byWordLevenshtein.similarity(analyze(attr), analyze(c))
    val maxVal = concepts.reduceOption[Double](max).getOrElse(0.0)
    maxVal
  }

  def termScore(req: REARequest, ds: Dataset) =
    jaccard(req.termSet, ds.termSet)

  def urlScore(req: REARequest, ds: Dataset) =
    jaccard(req.termSet, ds.urlTermSet)

  def titleScore(req: REARequest, ds: Dataset) =
    jaccard(req.ACTermSet, ds.titleTermSet)

  def domainTermScore(req: REARequest, ds: Dataset) =
    jaccard(req.termSet, ds.domainInfo.termSet) // TODO

  def predicateScore(req: REARequest, values: Seq[DrilledValue]) =
    if (req.predicates.isEmpty)
      1.0
    else {
      val result = req.predicates.map(singlePredicateScore(_, values)).sum / req.predicates.size.toDouble
      // println("for all preds: " + result)
      result
    }

  private def singlePredicateScore(p:Predicate, values:Seq[DrilledValue]) = p match {
    case p:NumericValuedPredicate => {
      // println("scoring for predicate "+p)
      val numericValues = values.map(_.value)
      val oomScore = numericValues.map(_ match {
        case d:java.lang.Double => {
          val d_safe = if (d != 0.0) d else new java.lang.Double(0.0000000001)
          // println(s"for val $d: ${log10(abs(d_safe))} ${abs(log10(abs(p.value)) - log10(abs(d_safe)))} ${(max(2.0 - (abs(log10(abs(p.value)) - log10(abs(d_safe)))), 0.0))}")
          (max(2.0 - (abs(log10(abs(p.value)) - log10(abs(d_safe)))), 0.0))
        }
        case _ => 0.0
      }).sum / (values.size.toDouble * 2.0)


      val selectivity = p.selectivity(numericValues)
      val selectivityScore = 1.0 - (2.0 * abs(0.5 - selectivity))
      // println(s"oomScore ${oomScore} selectivity ${selectivity} selectivityScore ${selectivityScore}")
      (oomScore + selectivityScore) / 2.0
    }
    case p:Predicate => {
      // println("scoring for predicate "+p)
      val realValues = values.map(_.value)
      val applicableScore = realValues.count(p.applicableForValue(_)).toDouble / values.size.toDouble
      val selectivity = p.selectivity(realValues)
      val selectivityScore = 1.0 - (2.0 * abs(0.5 - selectivity))
      // println(s"applicableScore ${applicableScore} selectivityScore ${selectivityScore}")
      (applicableScore + selectivityScore) / 2.0
    }
    case _ => 1.0
  }

  def domainScore(ds: Dataset) =
    Domains.scoreOnly(ds)
}
