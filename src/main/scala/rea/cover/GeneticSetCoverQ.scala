package rea.cover

import rea.definitions._
import rea.util.{unitMatrix, argMax, argMin}
import scala.math.{max, min}
import scala.collection.mutable
import scala.collection.mutable.{Buffer, ArrayBuffer}
import scala.annotation.tailrec
import scala.util.Random
import scala.language.implicitConversions
import scala.collection.JavaConversions._
import scala.collection.BitSet
import de.tudresden.matchtools.similarities._

abstract class GeneticSetCovererQ(k:Int, _entities: Seq[Int], _candidates: Seq[DrilledDataset],
    searchSpaceMultiplier: Int, thCons:Double = 0.0, thCov:Double = 0.0)
  extends GeneticSetCoverer(k, _entities, _candidates, searchSpaceMultiplier, thCons, thCov) {

  this: ResultScorer with ScoreWeighter with ResultSelector with Picker =>

  override def name = "GeneticSetCovererQ"
  override def toString = s"$name with $resultScorerName, $distanceMeasureName, $PickerName, $resultSelectorName"

  override def covers() = {

    var t = 0
    val basicCoverer = new InitialPopulationSetCoverer(k, entities, candidates, thCons, thCov)
        with ResultScorer
        with ScoreWeighter
        with InverseSimByEntityDistance
        with ExponentialDiversityPicker
    population = basicCoverer.covers().toArray

    // population = initPopulation()
    popSize = population.size
    println("initial popsize: "+popSize)
    distances = distMat()
    for (c <- population)
        updateUsages(c)

    assert(population.size == population.toSet[Cover].size)

    /* evolution */
    var creationCount = 0
    while (population.size > 4 && !stop(t)) {
      val s1 = rnd.nextInt(popSize)
      val (w1, l1) = tournament(s1)
      val others = population.indices.filter(i => i != w1 && i != l1)
      val s2 = others(rnd.nextInt(others.size))
      val (w2, _) = tournament(s2)

      val W1 = population(w1)
      val W2 = population(w2)
      val L1 = population(l1)

      val c1 = cross(w1, w2).toBuffer
      mutate(c1,1)
      forceFeasible(population.size, c1)
      val n = toCoverNonStrict(c1.map(candidates(_)))
      if(!population.contains(n)) {
        updateUsages(n)
        updateNegateUsages(L1)
        creationCount += 1
        replace(l1, n)
        assert(population.size == population.toSet[Cover].size)
      }
      t += 1
    }
    println("creation count: "+creationCount)
    val selectedCovers = select(population, k).map(pruneInconsistent).distinct
    selectedCovers.sortBy(score).reverse
  }

  override def stop(t: Int) = t > (k * searchSpaceMultiplier)
}