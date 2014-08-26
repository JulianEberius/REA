package rea.cover

import rea.definitions._
import rea.util.{unitMatrix, argMax, argMin, ProgressReporter, Nc}
import scala.math.{max, min, floor}
import scala.collection.mutable
import scala.collection.mutable.{Buffer, ArrayBuffer, ListBuffer}
import scala.annotation.tailrec
import scala.language.implicitConversions
import scala.collection.JavaConversions._
import scala.collection.BitSet
import de.tudresden.matchtools.similarities._


abstract trait ResultSelector {
  this:ResultScorer =>

  def resultSelectorName:String
  def select(xs: Seq[Cover], n:Int): Seq[Cover]
}



trait ExhaustiveResultSelector extends ResultSelector {
  this:ResultScorer with DistanceMeasure =>

  def resultSelectorName = "ExhaustiveResultSelector"
  def select(xs: Seq[Cover], k:Int): Seq[Cover] = {
    val reporter = new ProgressReporter(Nc(xs.size, k))

    // val xsl = xs.take(75)
    val best = xs.combinations(k).maxBy(ds => {
      reporter.progress()
      scoreSeq(ds)
    })

    if (best != null)
      best
    else
      Seq()
  }
}

trait DivLimExhaustiveResultSelector extends ResultSelector {
  this:ResultScorer with DistanceMeasure =>

  val divLim:Double = 0.8
  def resultSelectorName = "DivLimExhaustiveResultSelector"

  def select(xs: Seq[Cover], k:Int): Seq[Cover] = {
    val reporter = new ProgressReporter(Nc(xs.size, k))

    // val xsl = xs.take(75)
    println("divLim: "+divLim)
    val best = xs.combinations(k).map(c => (c, Cover.diversity(c, this))).filter(_._2 >= divLim-0.1).maxBy {
      case (c, div) => {
        reporter.progress()
        scoreSeq(c)
      }}

    if (best != null)
      best._1
    else
      Seq()
  }
}

trait GreedyResultSelector extends ResultSelector {
  this:ResultScorer =>

  def resultSelectorName = "GreedyResultSelector"
  def select(xs: Seq[Cover], k:Int): Seq[Cover] = {
    var buf = mutable.ListBuffer[Cover]() ++ xs
    var result = Set[Cover]()
    for (j <- 0 until min(k, buf.size)) {
      val pick = buf.maxBy(scoreWithRespectTo(_, result))
      buf -= pick
      result += pick
    }
    result.toSeq
  }
}
trait DiversifyingGreedyResultSelector extends ResultSelector {
  this:ResultScorer with DistanceMeasure =>

  def resultSelectorName = "DiversifyingGreedyResultSelector"
  def select(xs: Seq[Cover], k:Int): Seq[Cover] = {
    var buf = mutable.ListBuffer[Cover]() ++ xs
    var result = Set[Cover]()
    for (j <- 0 until min(k, buf.size)) {
      val pick = buf.maxBy(x => distance(x,result) * score(x))
      buf -= pick
      result += pick
    }
    result.toSeq
  }
}

trait AllResultSelector extends ResultSelector {
  this:ResultScorer =>

  def resultSelectorName = "AllResultSelector"
  def select(xs: Seq[Cover], k:Int): Seq[Cover] =
    xs
}

trait DiversityOnlyGreedyResultSelector extends ResultSelector {
  this:ResultScorer with DistanceMeasure =>

  def resultSelectorName = "DiversityOnlyGreedyResultSelector"
  def select(xs: Seq[Cover], k:Int): Seq[Cover] = {
    var buf = mutable.ListBuffer[Cover]() ++ xs
    var result = Set[Cover]()
    for (j <- 0 until min(k, buf.size)) {
      val pick = buf.maxBy(distance(_, result))
      buf -= pick
      result += pick
    }
    result.toSeq
  }
}
trait ScoreOnlyGreedyResultSelector extends ResultSelector {
  this:ResultScorer with DistanceMeasure =>

  def resultSelectorName = "ScoreOnlyGreedyResultSelector"
  def select(xs: Seq[Cover], k:Int): Seq[Cover] = {
    var buf = mutable.ListBuffer[Cover]() ++ xs
    var result = Set[Cover]()
    for (j <- 0 until min(k, buf.size)) {
      val pick = buf.maxBy(score(_))
      buf -= pick
      result += pick
    }
    result.toSeq
  }
}

trait CoherenceOnlyGreedyResultSelector extends ResultSelector {
  this:ResultScorer with DistanceMeasure =>

  def resultSelectorName = "ScoreOnlyGreedyResultSelector"
  def select(xs: Seq[Cover], k:Int): Seq[Cover] = {
    var buf = mutable.ListBuffer[Cover]() ++ xs
    var result = Set[Cover]()
    for (j <- 0 until min(k, buf.size)) {
      val pick = buf.maxBy(score(_))
      buf -= pick
      result += pick
    }
    result.toSeq
  }
}

trait ReplacingResultSelector extends ResultSelector {
  this:ResultScorer with ReplacementDecider =>
  val maxReplacementsFactor:Int

  override def resultSelectorName = s"ReplacingResultSelector($replacementDeciderName)"
  override def select(xs: Seq[Cover], k:Int): Seq[Cover] = {
    // initialize to top-k of multi-greedy
    val original = Set[Cover]() ++ xs.take(k)
    val cands = Set[Cover]() ++ xs.drop(k)
    val maxReplacements = k *maxReplacementsFactor

    @annotation.tailrec
    def _swap(result:Set[Cover], rCands:Set[Cover], aCands:Set[Cover], replacements:Int):Seq[Cover] = {
      if (replacements >= maxReplacements) {
        return result.toSeq
      }
      if (rCands.isEmpty) {
        return result.toSeq
      }
      if (aCands.isEmpty) {
        return result.toSeq
      }

      val rC = chooseRCandidate(result, rCands)
      val tmpBuf = result - rC
      val aC = chooseACandidate(tmpBuf, aCands)

      val newBuf = (result - rC) + aC//updated(result.indexOf(rC), aC)
      if (decide(tmpBuf, rC, aC) & !break(newBuf, original)) {
        return _swap(newBuf, rCands - rC, (aCands - aC), replacements + 1)
      }
      else {
        return _swap(result, rCands-rC, aCands, replacements + 1)
      }

    }
    _swap(original, original, cands, 0)
  }
}

trait ReplacingResultSelectorWithReplacement extends ResultSelector {
  this:ResultScorer with ReplacementDecider =>
  val maxReplacementsFactor:Int

  override def resultSelectorName = s"ReplacingResultSelectorWithReplacement($replacementDeciderName)"
  override def select(xs: Seq[Cover], k:Int): Seq[Cover] = {
    // initialize to top-k of multi-greedy
    val original = Set[Cover]() ++ xs.take(k)
    val cands = Set[Cover]() ++ xs.drop(k)
    val maxReplacements = k *maxReplacementsFactor

    @annotation.tailrec
    def _swap(result:Set[Cover], rCands:Set[Cover], aCands:Set[Cover], replacements:Int):Seq[Cover] = {
      if (replacements >= maxReplacements) {
        return result.toSeq
      }
      if (rCands.isEmpty) {
        return result.toSeq
      }
      if (aCands.isEmpty) {
        return result.toSeq
      }

      val rC = chooseRCandidate(result, rCands)
      val tmpBuf = result - rC
      val aC = chooseACandidate(tmpBuf, aCands)

      val newBuf = (result - rC) + aC//updated(result.indexOf(rC), aC)
      if (decide(tmpBuf, rC, aC) & !break(newBuf, original)) {
        return _swap(newBuf, newBuf, (aCands - aC) + rC, replacements + 1)
      }
      else {
        return _swap(result, rCands-rC, aCands, replacements + 1)
      }

    }
    _swap(original, original, cands, 0)
  }
}

trait ReplacingResultSelectorWithReplacementAndTabu extends ResultSelector {
  this:ResultScorer with DistanceMeasure with ReplacementDecider =>
  val maxReplacementsFactor:Int
  var tabuList:List[(Cover, Int)] = List()

  override def resultSelectorName = s"ReplacingResultSelectorWithReplacementAndTabu($replacementDeciderName)"
  override def select(xs: Seq[Cover], k:Int): Seq[Cover] = {
    // initialize to top-k of multi-greedy
    val original = Set[Cover]() ++ xs.take(k)
    val cands = Set[Cover]() ++ xs.drop(k)
    val maxReplacements = k *maxReplacementsFactor
    val tabuTime = k
    tabuList = List()

    @annotation.tailrec
    def _swap(result:Set[Cover], rCands:Set[Cover], aCands:Set[Cover], replacements:Int):Seq[Cover] = {
      if (replacements >= maxReplacements) {
        return result.toSeq
      }
      if (rCands.isEmpty) {
        return result.toSeq
      }
      if (aCands.isEmpty) {
        return result.toSeq
      }

      val rC = chooseRCandidate(result, rCands)
      val tmpBuf = result - rC
      val aC = chooseACandidate(tmpBuf, aCands)

      val newBuf = (result - rC) + aC//updated(result.indexOf(rC), aC)
      if (decide(tmpBuf, rC, aC) && !break(newBuf, original)) {
        val tabuUntil:Int = floor(replacements + (k/2.0)).toInt
        tabuList ::= (rC, tabuUntil)
        val unTabued = tabuList.filter(_._2 <= replacements)
        tabuList = tabuList.diff(unTabued)
        return _swap(newBuf, newBuf, (aCands - aC) ++ unTabued.map(_._1), replacements + 1)
      }
      else {
        return _swap(result, rCands-rC, aCands, replacements + 1)
      }

    }
    _swap(original, original, cands, 0)
  }
}

trait ReplacementDecider {
  def replacementDeciderName:String
  def chooseRCandidate(result:Set[Cover], rCands:Set[Cover]): Cover
  def chooseACandidate(result:Set[Cover], aCands:Set[Cover]): Cover
  def decide(result:Set[Cover], rCand:Cover, aCand:Cover): Boolean
  def break(result:Set[Cover], original:Set[Cover]): Boolean = false

  val maxReplacementsFactor = 10
}

trait ReplacementDecider13 extends ReplacementDecider {
  this:ResultScorer with DistanceMeasure =>

  def replacementDeciderName = "ReplacementDecider13"
  def chooseRCandidate(result:Set[Cover], rCands:Set[Cover]): Cover =
    rCands.minBy(c => score(c) * distance(c, result))
  def chooseACandidate(result:Set[Cover], aCands:Set[Cover]): Cover =
    aCands.maxBy(c => score(c) * distance(c, result))
  def decide(result:Set[Cover], rC:Cover, aC:Cover): Boolean = {
    val x = (
                Cover.diversity((result + aC).toSeq, this)
              - Cover.diversity((result + rC).toSeq, this)
            ) + (
                score(aC)
              - score(rC)
            ) > 0.0
    x
  }

  val breakThreshold = 0.95
  override def break(result:Set[Cover], original:Set[Cover]): Boolean = {
    val divLoss = (Cover.diversity(result.toSeq, this) <  breakThreshold * Cover.diversity(original.toSeq, this))
    // val covLoss = (Cover.coverage(result.toSeq, numEntities) < breakThreshold * Cover.coverage(original.toSeq, numEntities))
    // val scoreLoss = (Cover.score(result.toSeq, this) < breakThreshold * Cover.score(original.toSeq, this))
    divLoss//  || covLoss
  }
}


trait NoReplacementDecider extends ReplacementDecider {
  this:ResultScorer with DistanceMeasure =>

  override val maxReplacementsFactor = 0

  def replacementDeciderName = "NoReplacementDecider"
  def chooseRCandidate(result:Set[Cover], rCands:Set[Cover]): Cover =
    rCands.minBy(c => score(c))
  def chooseACandidate(result:Set[Cover], aCands:Set[Cover]): Cover =
    aCands.maxBy(c => score(c))
  def decide(result:Set[Cover], rC:Cover, aC:Cover): Boolean =
    false
}
