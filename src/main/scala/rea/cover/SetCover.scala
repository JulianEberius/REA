package rea.cover

import rea.definitions._
import rea.coherence.Coherence
import scala.math.{min, max}
import scala.collection.mutable
import rea.util.{unitMatrix, Nn, Nc, ProgressReporter, bitSetJaccard}
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.collection.BitSet
import de.tudresden.matchtools.similarities._

trait ScoreWeighter {
  val wInherentScore: Double = 1.0
  val wCoverage: Double = 1.0
  val wCoherence: Double = 1.0
  val wDiversity: Double = 1.0
  val wMinSources:Double = 1.0
  val wGlobalScore: Double = 1.0
}

trait ResultScorer {
  self: DistanceMeasure =>

  def resultScorerName() = "DefaultResultScorer"
  type CMat = Array[Array[Double]]
  val cMat:CMat
  val numEntities:Int
  val numCandidates:Int

  val scoreCache = mutable.HashMap[Cover, Double]()

  def score(r:Cover): Double =
    // scoreCache.getOrElseUpdate(r,
    //   r.consistency(cMat) + r.inherentScore() + r.coverQuality(numEntities))//+ r.minSources() + r.coverage(numEntities))
    // r.consistency(cMat) + r.inherentScore() + r.coverQuality(numEntities)
    r.consistency(cMat) + r.inherentScore() + r.minSources() + r.coverage(numEntities)


  def scoreWithRespectTo(x: Cover, xs:Set[Cover]): Double =
    distance(x, xs) + score(x)

  def scoreSeq(xs: Seq[Cover]): Double =
    Cover.score(xs, this) + Cover.diversity(xs, this)// + Cover.coverage(xs, numEntities)
}

abstract trait DistanceMeasure {
  self:SetCoverer =>

  def distanceMeasureName():String
  def distance(a:Cover, b:Cover):Double

  def _distance(a:Cover, others:Set[Cover]):Double =
    if (others.size == 0)
      1.0
    else
      others.map(distance(a, _)).sum / others.size

  // def distanceNoSelf(a:Cover, others:Set[Cover]):Double =
  def distance(a:Cover, others:Set[Cover]):Double =
    if (others.contains(this))
      0.0
    else
      _distance(a, others)
}

trait InverseSimDistance extends DistanceMeasure {
  self:SetCoverer =>

  def distanceMeasureName = "InverseSimDistance"
  def distance(a:Cover, b:Cover):Double = {
    var cnt = 0
    var sum = 0.0
    for (i <- a.datasetIndices; j <- b.datasetIndices) {
        sum += cMat(i)(j)
        cnt += 1
    }
    if (cnt == 0)
        1.0
    else
        1.0 - (sum / cnt.toDouble)
  }
}

trait InverseSimByEntityDistance extends DistanceMeasure {
  self:SetCoverer =>

  def distanceMeasureName = "InverseSimByEntityDistance"
  def distance(a:Cover, b:Cover):Double = {
    var cnt = 0
    var sum = 0.0
    for (ei <- entities) {
      val va = a.valByEntity.get(ei)
      val vb = b.valByEntity.get(ei)
      if (va.isDefined && vb.isDefined) {
        val dsia = candIndex(va.get.dataset)
        val dsib = candIndex(vb.get.dataset)
        sum += cMat(dsia)(dsib)
      }
      else
        sum += 0.0
    }
      1.0 - (sum / numEntities)
  }

}

object SetCoverer {
  def printCovers(covers:Seq[Cover], coverer: SetCoverer, entities: Array[String]) = {
    for ((c, i) <- covers.zipWithIndex) {
      println(s"$i)\n--")
      println(c.toString(entities))
      println("cov: " + c.coverage(coverer.numEntities))
      // println("score: " + c.inherentScore)
      println("cons: " + c.consistency(coverer.cMat))
      // println(c.datasets.map(d => d.scores.toString + "\n" +
      //   s"(${d.scores.attSim} + ${d.scores.entitySim} * 0.9 + ${d.scores.conceptSim} * 0.25 + ${d.scores.termSim} * 0.25 + ${d.scores.titleSim} * 0.25 + ${d.scores.coverageA} + ${d.scores.coverageB} * 0.5  + ${d.scores.domainPopularity} * 0.8)"+
      //   "\n" + s"${d.scores.score} + ${d.scores.inherentScore}").mkString("\n"))
      // println(c.datasets.map(d => d.scores.toString).mkString("\n"))
      println
    }
  }
}

trait UsageJaccardDistance extends DistanceMeasure {
  self:SetCoverer =>

  val jaccard = new SetSimilarities.GoogleJaccard[DrilledValue]()

  def distanceMeasureName = "UsageJaccardDistance"
  def distance(a:Cover, b:Cover):Double =
    1.0 - jaccard.similarity(a.valSet, b.valSet)
    // 1.0 - bitSetJaccard(a.valIdSet, b.valIdSet)
}

abstract class SetCoverer(val k:Int, val entities: Seq[Int], val candidates: Seq[DrilledDataset])
extends ResultScorer with DistanceMeasure {

  val numEntities = entities.size
  val numCandidates = candidates.size
  val candidateIndices = candidates.indices
  val candIndex: Map[Dataset, Int] = candidates.map(_.dataset).zipWithIndex.toMap
  val cMat = Coherence.coherenceMatrix(candidates)

  def covers(): Seq[Cover]

  def name():String

  def toCover(xs:Iterable[DrilledDataset]):Option[Cover] = {
    val ds = xs.toSeq
    val freeSet = new java.util.BitSet(entities.size)
    freeSet.flip(0, entities.size)

    val picks = mutable.ListBuffer[Seq[DrilledValue]]()
    for (x <- ds) {
      val newVals = mutable.ListBuffer[DrilledValue]()
      for (v <- x.values) {
        if (freeSet.get(v.forIndex)) {
          freeSet.clear(v.forIndex)
          newVals += v
        }
      }
      if (newVals.isEmpty)
        return None
      picks += newVals
    }

    if (freeSet.isEmpty)
      return Some(new Cover(ds, ds.map(ds => candIndex(ds.dataset)), picks))
    else
      return None
  }


  def toCoverNonStrict(xs:Iterable[DrilledDataset]):Cover = {
    val ds = xs.toSeq
    val freeSet = mutable.BitSet() ++ entities

    val newDS = mutable.ListBuffer[DrilledDataset]()
    val picks = mutable.ListBuffer[Seq[DrilledValue]]()
    for (x <- ds) {
      val newVals = mutable.ListBuffer[DrilledValue]()
      for (v <- x.values) {
        if (freeSet(v.forIndex)) {
          freeSet -= v.forIndex
          newVals += v
        }
      }
      if (! newVals.isEmpty) {
        newDS += x
        picks += newVals
      }
    }
    return new Cover(newDS, newDS.map(d => candIndex(d.dataset)), picks)
  }

  def newValues(dsi: Int, freeEntities: BitSet):Seq[DrilledValue] =
      candidates(dsi).values.filter(v => freeEntities(v.forIndex))

  /** Returns the average coherence of the given dataset with the already selected ones */
  protected def coherenceWith(dsi: Int, selectedDs: Seq[Int]) =
    if (selectedDs.size > 0) {
      val col:Array[Double] = cMat(dsi)
      var cSum = 0.0
      for (oi <- selectedDs) {
        cSum += col(oi)
      }
      (cSum / selectedDs.size)
    } else
      1.0

  protected def avgConsistency(xs: Seq[Int]) =
    if (xs.size > 1) {
      xs.combinations(2).map(c => cMat(c(0))(c(1))).sum / ((xs.size * (xs.size - 1)) / 2.0)
    } else
      1.0

  def createW(i1:Seq[Int]):mutable.Map[Int, Int] = {
    // val w = Array.fill(numEntities)(0)
    val w = mutable.Map[Int, Int]().withDefaultValue(0)
    for (i <- i1) {
      candidates(i).coveredEntities.foreach(w(_) += 1)
    }
    w
  }
  def isRedundant(w:mutable.Map[Int, Int], i:Int) = candidates(i).coveredEntities.forall(w(_) >= 2)

  def pruneInconsistent(c:Cover):Cover = {
    c
    // var stop = false
    // val i = mutable.Buffer() ++ c.datasetIndices
    // val w = createW(i)
    // while (!stop) {
    //   val redundant = i.filter(isRedundant(w, _))
    //   if (redundant.isEmpty)
    //     stop = true
    //   else {
    //     val dsi = redundant.minBy(x => coherenceWith(x, i - x))
    //     candidates(dsi).coveredEntities.foreach(w(_) -= 1)
    //     i -= dsi
    //   }
    // }
    // if (i != c.datasetIndices) {
    //   // println("PRUNED COVER!!!!!!")
    //   val newC = toCoverNonStrict(i.map(candidates(_)))
    //   newC
    // }
    // else
    //   c
  }
}
