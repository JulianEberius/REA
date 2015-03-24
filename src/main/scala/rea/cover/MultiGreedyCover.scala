package rea.cover

import rea.definitions._
import rea.util.{unitMatrix, argMax, argMin}
import scala.math.{max, min}
import scala.collection.mutable
import scala.collection.mutable.{Buffer, ArrayBuffer}
import scala.annotation.tailrec
import scala.language.implicitConversions
import scala.collection.JavaConversions._
import scala.collection.BitSet
import de.tudresden.matchtools.similarities._

abstract class MultiGreedySetCoverer(k:Int, _entities: Seq[Int],
  _candidates: Seq[DrilledDataset], val searchSpaceMultiplier: Int, thCons:Double = 0.0, thCov:Double = 0.0)
  extends GreedySetCoverer(k, _entities, _candidates, thCons, thCov) {

  this: ResultScorer with ScoreWeighter with Picker with ResultSelector =>

  override def name = "MultiGreedySetCoverer"
  override def toString = s"$name with $resultScorerName, $distanceMeasureName, $PickerName, $resultSelectorName"

  /* Parameter */
  val constK = 10 // for testing
  // override val maxCovers = constK*searchSpaceMultiplier
  // override val maxIterations = constK*searchSpaceMultiplier*10
  override val maxCovers = k*searchSpaceMultiplier
  override val maxIterations = k*searchSpaceMultiplier*10

  override def covers(): Seq[Cover] = {
    val covers = createCovers(0, List(), 0)
    println(s"found ${covers.size} covers")
    // println("USAGE: ")
    // printUsages()

    val selectedCovers = select(covers, k).map(pruneInconsistent).distinct
    selectedCovers.sortBy(score).reverse
  }

  protected def createCovers(iteration: Int, covers: List[Cover], lastChange:Int): Seq[Cover] = {
    // stopping condition
    if (covers.size == maxCovers || iteration == maxIterations || lastChange == 10) {
      // println(s"finishing greedy*: ${covers.size}/${maxCovers} ${iteration}/${maxIterations} ${lastChange}/10")
      return covers.reverse.toSeq
    }

    // create new cover, record used datasets
    val nc = newCover(iteration)
    updateUsages(nc)

    // add to solution set (if new), recurse
    return if (!covers.contains(nc))
        createCovers(iteration+1, nc :: covers, 0)
      else {
        createCovers(iteration+1, covers, lastChange+1)
      }
  }


}