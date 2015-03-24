package rea.cover

import rea.definitions._
import rea.coherence.Coherence
import rea.util.{unitMatrix, argMax, argMin}
import scala.math.{max, min, pow}
import scala.collection.mutable
import scala.collection.mutable.{Buffer, ArrayBuffer}
import scala.annotation.tailrec
import scala.language.implicitConversions
import scala.collection.JavaConversions._
import scala.collection.BitSet
import de.tudresden.matchtools.similarities._

abstract trait Picker {
  this: ResultScorer with SetCoverer with ScoreWeighter =>

  val usages:Map[Int, Array[Int]]

  def PickerName:String
  def diversityWith(iteration:Int, dsi:Int, freeSet:BitSet):Double

  def printUsages() {
    for ((e, l) <- usages)
      for ((u,ui) <- l.zipWithIndex)
        if (u > 0)
          println(s"$e,$ui -> $u")
        // println(l.map("%02d".format(_)).mkString(" "))
  }

  def scorePick(iteration:Int, freeEntities:BitSet, selected:Seq[Int], completeness:Double = 1.0)(c:DrilledDataset, ci: Int):Double = {
    val freeSize = freeEntities.size.toDouble
    val coverable = (c.coveredEntities & freeEntities)
    val coverageScore = coverable.size / freeSize

    if (coverageScore > 0) {
    val divWith = diversityWith(iteration, ci, coverable) + 0.00001
    val tmp = (wInherentScore * c.inherentScore +
    		wCoverage * coverageScore +
    		wCoherence * coherenceWith(ci, selected)) * // div is multiplied!
    		(wDiversity * divWith)
      tmp
    }
    else
      0.0
  }
}

trait UsagePicker extends Picker with ResultScorer {
  this: DistanceMeasure with SetCoverer with ScoreWeighter =>

  def PickerName = "UsagePicker"
  def diversityWith(iteration:Int, dsi: Int, freeSet:BitSet): Double = {
    if (iteration == 0)
      return 1.0

    var cSum = 0.0
    val normalizer = freeSet.size * iteration
    for (fe <- freeSet)
      cSum += usages(fe)(dsi)
    if (normalizer == 0)
      return 1.0
    val tmp = 1.0 - (cSum / normalizer.toDouble)
    assert(tmp >= 0.0)
    return tmp
  }
}

trait ExponentialUsagePicker extends Picker with ResultScorer {
  this: DistanceMeasure with SetCoverer with ScoreWeighter =>

  def PickerName = "ExponentialUsagePicker"
  def diversityWith(iteration:Int, dsi: Int, freeSet:BitSet): Double = {
    if (iteration == 0)
      return 1.0

    var cSum = 0.0
    val normalizer = freeSet.size * iteration
    for (fe <- freeSet)
      cSum += usages(fe)(dsi)

    if (normalizer == 0)
      return 1.0
    val tmp = pow(1.0 - (cSum / normalizer.toDouble), iteration)
    assert(tmp >= 0.0)
    return tmp
  }
}

trait NoReusePicker extends Picker with ResultScorer {
  this: DistanceMeasure with SetCoverer with ScoreWeighter =>

  def PickerName = "NoReusePicker"
  def diversityWith(iteration:Int, dsi: Int, freeSet:BitSet): Double = {
    if (iteration == 0)
      return 1.0

    var cSum = 0.0
    val normalizer = freeSet.size * iteration
    for (fe <- freeSet)
      if (usages(fe)(dsi) > 0)
        return 0.0
    return 1.0
  }
}

trait DiversityPicker extends Picker with ResultScorer {
  this: DistanceMeasure with SetCoverer with ScoreWeighter =>

  def PickerName = "DiversityPicker"
  def diversityWith(iteration:Int, dsi: Int, freeSet:BitSet): Double = {
    if (iteration == 0)
      return 1.0

    var cSum = 0.0
    var normalizer = 0
    for (fe <- freeSet) {
      val fUsages = usages(fe)
      var i = 0
      while (i < numCandidates) {
        val fUsagesI = fUsages(i)
        if (fUsagesI > 0) {
          cSum += cMat(dsi)(i)*fUsagesI
          normalizer += fUsagesI
        }
        i += 1
      }
    }
    if (normalizer == 0)
      return 1.0
    val tmp = 1.0 - (cSum / normalizer.toDouble)
    assert(tmp >= 0.0)
    return tmp
  }
}

trait ExponentialDiversityPicker extends Picker with ResultScorer {
  this: DistanceMeasure with SetCoverer with ScoreWeighter =>

  def PickerName = "ExponentialDiversityPicker"
  def diversityWith(iteration:Int, dsi: Int, freeSet:BitSet): Double = {
    if (iteration == 0)
      return 1.0

    val col = cMat(dsi)
    var cSum = 0.0
    var normalizer = 0
    for (fe <- freeSet) {
      val fUsages = usages(fe)
      var i = 0
      while (i < numCandidates) {
        val fUsagesI = fUsages(i)
        if (fUsagesI > 0) {
          cSum += col(i)*fUsagesI
          normalizer += fUsagesI
        }
        i += 1
      }
    }
    if (normalizer == 0)
      return 1.0
    val tmp = pow(1.0 - (cSum / normalizer.toDouble), iteration)
    assert(tmp >= 0.0)
    return tmp
  }
}

trait DiversityDynamicPicker extends DiversityPicker {
  this: DistanceMeasure with SetCoverer with ScoreWeighter =>

  override def PickerName = "DiversityDynamicPicker"
  override def scorePick(iteration:Int, freeEntities:BitSet, selected:Seq[Int], completeness:Double = 1.0)(c:DrilledDataset, ci: Int):Double = {
    val freeSize = freeEntities.size.toDouble
    val coverable = (c.coveredEntities & freeEntities)
    val coverageScore = coverable.size / freeSize

    if (coverageScore > 0)
      (wInherentScore * c.inherentScore +
        wCoverage * (1.0 - completeness) * coverageScore +
        wCoherence * completeness * coherenceWith(ci, selected)) *
      wDiversity * (1.0 - completeness) * diversityWith(iteration, ci, coverable)
    else
      0.0
  }
}

trait ExponentialDiversityDynamicPicker extends ExponentialDiversityPicker {
  this: DistanceMeasure with SetCoverer with ScoreWeighter =>

  override def PickerName = "ExponentialDiversityDynamicPicker"
  override def scorePick(iteration:Int, freeEntities:BitSet, selected:Seq[Int], completeness:Double = 1.0)(c:DrilledDataset, ci: Int):Double = {
    val freeSize = freeEntities.size.toDouble
    val coverable = (c.coveredEntities & freeEntities)
    val coverageScore = coverable.size / freeSize

    if (coverageScore > 0)
      (c.inherentScore +
        coverageScore * 0.5 * (1.0 - completeness) +
        (cMat(ci).sum / cMat.size) * (1.0 - completeness) +
        coherenceWith(ci, selected) * completeness) *
        ((1.0 - completeness) * diversityWith(iteration, ci, coverable))
    else
      0.0
  }
}

trait ScoreOnlyPicker extends ExponentialDiversityPicker {
  this: DistanceMeasure with SetCoverer with ScoreWeighter =>

  override def PickerName = "ScoreOnlyExponentialDiversityPicker"
  override def scorePick(iteration:Int, freeEntities:BitSet, selected:Seq[Int], completeness:Double = 1.0)(c:DrilledDataset, ci: Int):Double = {
    val freeSize = freeEntities.size.toDouble
    val coverable = (c.coveredEntities & freeEntities)
    val coverageScore = coverable.size / freeSize

    if (coverageScore > 0)
      c.inherentScore *
      diversityWith(iteration, ci, coverable)
    else
      0.0
  }
}

trait CoherenceOnlyPicker extends ExponentialDiversityPicker {
  this: DistanceMeasure with SetCoverer with ScoreWeighter =>

  override def PickerName = "CoherenceOnlyExponentialDiversityPicker"
  override def scorePick(iteration:Int, freeEntities:BitSet, selected:Seq[Int], completeness:Double = 1.0)(c:DrilledDataset, ci: Int):Double = {
    val freeSize = freeEntities.size.toDouble
    val coverable = (c.coveredEntities & freeEntities)
    val coverageScore = coverable.size / freeSize

    if (coverageScore > 0)
      coherenceWith(ci, selected) *
      diversityWith(iteration, ci, coverable)
    else
      0.0
  }
}

trait DiversityOnlyPicker extends ExponentialDiversityPicker {
  this: DistanceMeasure with SetCoverer with ScoreWeighter =>

  override def PickerName = "DiversityOnlyExponentialDiversityPicker"
  override def scorePick(iteration:Int, freeEntities:BitSet, selected:Seq[Int], completeness:Double = 1.0)(c:DrilledDataset, ci: Int):Double = {
    val freeSize = freeEntities.size.toDouble
    val coverable = (c.coveredEntities & freeEntities)
    val coverageScore = coverable.size / freeSize

    if (coverageScore > 0)
      diversityWith(iteration, ci, coverable)
    else
      0.0
  }
}

trait ScoreOnlyUsagePicker extends ExponentialUsagePicker {
  this: DistanceMeasure with SetCoverer with ScoreWeighter =>

  override def PickerName = "ScoreOnlyExponentialUsagePicker"
  override def scorePick(iteration:Int, freeEntities:BitSet, selected:Seq[Int], completeness:Double = 1.0)(c:DrilledDataset, ci: Int):Double = {
    val freeSize = freeEntities.size.toDouble
    val coverable = (c.coveredEntities & freeEntities)
    val coverageScore = coverable.size / freeSize

    if (coverageScore > 0)
      c.inherentScore *
      diversityWith(iteration, ci, coverable)
    else
      0.0
  }
}

trait CoherenceOnlyUsagePicker extends ExponentialUsagePicker {
  this: DistanceMeasure with SetCoverer with ScoreWeighter =>

  override def PickerName = "CoherenceOnlyExponentialUsagePicker"
  override def scorePick(iteration:Int, freeEntities:BitSet, selected:Seq[Int], completeness:Double = 1.0)(c:DrilledDataset, ci: Int):Double = {
    val freeSize = freeEntities.size.toDouble
    val coverable = (c.coveredEntities & freeEntities)
    val coverageScore = coverable.size / freeSize

    if (coverageScore > 0)
      coherenceWith(ci, selected) *
      diversityWith(iteration, ci, coverable)
    else
      0.0
  }
}

trait DiversityOnlyUsagePicker extends ExponentialUsagePicker {
  this: DistanceMeasure with SetCoverer with ScoreWeighter =>

  override def PickerName = "DiversityOnlyExponentialUsagePicker"
  override def scorePick(iteration:Int, freeEntities:BitSet, selected:Seq[Int], completeness:Double = 1.0)(c:DrilledDataset, ci: Int):Double = {
    val freeSize = freeEntities.size.toDouble
    val coverable = (c.coveredEntities & freeEntities)
    val coverageScore = coverable.size / freeSize

    if (coverageScore > 0)
      diversityWith(iteration, ci, coverable)
    else
      0.0
  }
}

abstract class GreedySetCoverer(k:Int, _entities: Seq[Int], _candidates: Seq[DrilledDataset],
  val thCons:Double = 0.0, val thCov:Double = 0.8) extends SetCoverer(k, _entities, _candidates) {

  this: ResultScorer with ScoreWeighter with Picker =>

  def name = "GreedySetCoverer"
  override def toString = s"$name with $resultScorerName, $distanceMeasureName, $PickerName"

  /* State */
  val usages:Map[Int, Array[Int]] = _entities.map(e => (e, Array.fill(numCandidates)(0))).toMap //Array.fill(numEntities, numCandidates)(0)

  val maxCovers = k
  val maxIterations = k*10

  override def covers(): Seq[Cover] = {
    val covers:Seq[Cover] = createCovers(0, List())
    println(s"found ${covers.size} covers")
    // println("USAGE: ")
    // printUsages()
    covers.map(pruneInconsistent).distinct
  }

  protected def createCovers(iteration: Int, covers: List[Cover]): Seq[Cover] = {
    // stopping condition
    if (covers.size == maxCovers || iteration == maxIterations) {
      return covers.reverse.toSeq
    }

    // create new cover, record used datasets
    val nc = newCover(iteration)
    updateUsages(nc)

    // add to solution set (if new), recurse
    return if (!covers.contains(nc))
        createCovers(iteration+1, nc :: covers)
      else {
        createCovers(iteration+1, covers)
      }
  }

  protected def updateUsages(newCover:Cover): Unit =
    for ((dsi, picks) <- newCover.datasetIndices.zip(newCover.picks)) {
      val covered = picks.map(_.forIndex)
      covered.foreach(usages(_)(dsi) += 1)
    }


  protected def newCover(iteration:Int): Cover = {

    @tailrec
    def _cover(
      selected: Seq[Int],
      values: Seq[Seq[DrilledValue]],
      freeEntities: BitSet,
      consistency:Double): Cover = {

      val freeSize = freeEntities.size.toDouble
      if (freeSize == 0.0) {
        return new Cover(selected.map(candidates(_)), selected, values)
      }

      val completeness = 1.0 - (freeSize / numEntities)
      // val c = candidates.maxBy(c => scorePick(c, completeness))
      val scorer = scorePick(iteration, freeEntities, selected, completeness)_
      val scoredCands = candidates.zipWithIndex.map { case (c,ci) => (ci, scorer(c, ci)) }
      val (bestIdx:Int, score:Double) = scoredCands.maxBy(_._2)


      if (score == 0.0) {
        return new Cover(selected.map(candidates(_)), selected, values)
      }
      val newConsistency = avgConsistency(selected :+ bestIdx)
      if(newConsistency < thCons && completeness >= thCov) {
        return new Cover(selected.map(candidates(_)), selected, values)
      }

      val newVals = newValues(bestIdx, freeEntities)
      val covered = newVals.map(_.forIndex)

      return _cover(
        selected :+ bestIdx,
        values :+ newVals,
        freeEntities -- covered,
        newConsistency)
    }

    _cover(Vector(), Vector(), BitSet.empty ++ entities, 1.0)
  }

}
