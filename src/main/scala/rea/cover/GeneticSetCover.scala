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

abstract class GeneticSetCoverer(k:Int, val _entities: Seq[Int], _candidates: Seq[DrilledDataset],
    val searchSpaceMultiplier: Int, thCons:Double = 0.0, thCov:Double = 0.0)
  extends GreedySetCoverer(k, _entities, _candidates, thCons, thCov) {

  this: ResultScorer with ScoreWeighter with Picker with ResultSelector =>

  override def name = "GeneticSetCoverer"
  override def toString = s"$name with $resultScorerName, $distanceMeasureName, $PickerName, $resultSelectorName"

  val rnd = new Random(0)

  /* Parameter */
  val MAX_GENERATIONS = (k * 2.5 * searchSpaceMultiplier).toInt
  val MAX_INIT_POP = (k * 0.15 * searchSpaceMultiplier).toInt
  // val MAX_GENERATIONS = (50 * searchSpaceMultiplier * 0.5).toInt
  // val MAX_INIT_POP = (3 * searchSpaceMultiplier * 0.5).toInt

  /* helper structures */
  // for each e, candidates that cover e
  val alpha = _entities.map(e => (e,candidateIndices.filter(candidates(_).coveredEntities(e)))).toMap
  // for each dsi (index), _entities covered by dataset(dsi)
  val beta = candidateIndices.map(candidates(_).coveredEntities)

  /* State */
  var population:Array[Cover] = null
  var popSize:Int = -1
  var distances:Array[Array[Double]] = null

  override def covers() = {

    var t = 0
    population = initPopulation()
    popSize = population.size
    distances = distMat()
    for (c <- population)
        updateUsages(c)

    /* evolution */
    var creationCount = 0
    while (!stop(t)) {
      // val scoreSum = population.map(score(_)).sum

      val (w1, l1) = tournament(rnd.nextInt(popSize))
      val (w2, l2) = tournament(rnd.nextInt(popSize))
      // val s1 = pickStarter(scoreSum, population)
      // var s2 = s1
      // while (s2 == s1) {
      //   s2 = pickStarter(scoreSum, population)
      // }
      // val (w1, l1) = tournament(s1)
      // val (w2, l2) = tournament(s2)
      val W1 = population(w1)
      val W2 = population(w2)
      val L1 = population(l1)
      val L2 = population(l2)
      val c1 = cross(w1, w2)
      mutate(c1,10)
      forceFeasible(population.size, c1)
      val n = toCoverNonStrict(c1.map(candidates(_)))

      if(!population.contains(n)) {
        updateUsages(n)
        updateNegateUsages(L1)
        creationCount += 1
        replace(l1, n)
      }
      t += 1
    }
    val selectedCovers = select(population, k).map(pruneInconsistent).distinct
    selectedCovers.sortBy(score).reverse
  }

  protected def pickStarter(scoreSum:Double, population:Seq[Cover]):Int = {
    var c = 0.0
    val x = rnd.nextDouble * scoreSum
    for ((p, pi) <- population.zipWithIndex) {
        c += score(p)
        if (c > x)
            return pi
    }
    assert(false) // should never be reached
    return 0
  }

  protected def updateNegateUsages(oldCover:Cover): Unit =
    for ((dsi, picks) <- oldCover.datasetIndices.zip(oldCover.picks)) {
      val covered = picks.map(_.forIndex)
      covered.foreach(usages(_)(dsi) -= 1)
    }

  def stop(t: Int) = t > MAX_GENERATIONS

  def initPopulation() = {
    val cnts = Array.fill(numCandidates)(0)
    // val pop = mutable.ArrayBuffer[Individual]()
    val pop = mutable.HashSet[Seq[Int]]()
    var nothingFoundSince = 0
    while (!cnts.forall(_ > 0) && pop.size < MAX_INIT_POP && nothingFoundSince < 10) {
      val i = random()
      if (!pop.contains(i)) {
        i.foreach(cnts(_) += 1)
        pop += i
        nothingFoundSince = 0
      }
      else { nothingFoundSince += 1 }
    }
    pop.map(si => si.map(candidates(_))).map(toCoverNonStrict).toArray
  }

  def random() = {
    val individual = mutable.ArrayBuffer[Int]()
    // random solution from alpha_k
    val shuffledEntities = Random.shuffle(entities)
    for (e <- shuffledEntities) {
      val al = alpha(e)
      val dsi = al(rnd.nextInt(al.size))
      individual += dsi
    }
    val w = createW(individual)
    def isRedundant(i:Int) = beta(i).forall(w(_) >= 2)
    // prune to minimal solutionas
    val shuffled = rnd.shuffle(individual)
    for (dsi <- shuffled) {
      if (isRedundant(dsi)) {
        individual -= dsi
        beta(dsi).foreach(w(_) -= 1)
      }
    }

    individual.toSeq
  }
  /* a fight to the death between an individual and its most similar solution */
  def tournament(li:Int) = {
    val oi = argMin(distances(li))
    // println(s"fight of $oi (${population(oi).datasetIndices}) vs $li (${population(li).datasetIndices})")
    if (score(population(li)) > score(population(oi)))
      (li, oi)
    else
      (oi, li)
  }

  def replace(i:Int,r:Cover):Unit = {
    population(i) = r
    updateDistVec(i)
  }

  def cross(p1:Int, p2:Int):mutable.Buffer[Int] = {
    val i1 = population(p1)
    val i2 = population(p2)
    // consistentFusion(i1, i2)
    fusion(i1, i2)
    // simpleCross(i1,i2)
  }

  override def createW(i1:Seq[Int]) = {
    val w = mutable.Map[Int, Int]().withDefaultValue(0)
    for (i <- i1) {
      beta(i).foreach(w(_) += 1)
    }
    w
  }

  def forceFeasible(iteration:Int, i1:mutable.Buffer[Int]) = {
    val w = createW(i1)

    def coverUncoveredLocal(U: mutable.BitSet) = {

      while (!U.isEmpty) {
        val u = U.head
        val freeSize = U.size
        val dsi = alpha(u).maxBy(dsi => {
          val coverable = (beta(dsi) & U)
          val coverageScore = coverable.size / freeSize
          coverageScore * candidates(dsi).score * diversityWith(iteration,dsi,coverable)
        })

        beta(dsi).foreach(w(_) += 1)
        i1 += dsi
        U &~= beta(dsi) // &~ is and-not
      }
    }
    def coverUncoveredConsistent(U: mutable.BitSet) = {
      while (!U.isEmpty) {
        val u = U.head
        val freeSize = U.size
        val dsi = alpha(u).maxBy(dsi => {
          val coverable = (beta(dsi) & U)
          val coverageScore = coverable.size / freeSize
          coverageScore * consistencyWith(dsi) * diversityWith(iteration,dsi,coverable)
        })

        i1 += dsi
        beta(dsi).foreach(w(_) += 1)
        U &~= beta(dsi) // &~ is and-not
      }
    }
    def coverUncoveredNewTh(U: mutable.BitSet) = {
      val completeness = 1.0 - (U.size / numEntities.toDouble)
      var stop = avgConsistency(i1) < thCons && completeness >= thCov

      while (!U.isEmpty && !stop) {
        val u = U.head
        val freeSize = U.size
        val completeness = 1.0 - (freeSize / numEntities.toDouble)
        val scorer = scorePick(iteration, U, i1, completeness)_
        val scoredCands:Seq[(Int, Double)] = alpha(u).map { case (ci) => (ci, scorer(candidates(ci), ci)) }
        val (dsi:Int, score:Double) = scoredCands.maxBy(_._2)

        beta(dsi).foreach(w(_) += 1)
        U &~= beta(dsi) // &~ is and-not

        val newConsistency = avgConsistency(i1 :+ dsi)

        if(newConsistency < thCons && completeness >= thCov ) {
          stop = true
        } else {
          i1 += dsi
        }

      }
    }
    def coverUncoveredNew(U: mutable.BitSet) = {
      while (!U.isEmpty) {
        val u = U.head
        val freeSize = U.size
        val completeness = 1.0 - (freeSize / numEntities.toDouble)
        val scorer = scorePick(iteration, U, i1, completeness)_
        val scoredCands:Seq[(Int, Double)] = alpha(u).map { case (ci) => (ci, scorer(candidates(ci), ci)) }
        val (dsi:Int, score:Double) = scoredCands.maxBy(_._2)

        i1 += dsi
        beta(dsi).foreach(w(_) += 1)
        U &~= beta(dsi) // &~ is and-not
      }
    }
    def pruneRandomized(): Unit = {
      val ir = rnd.shuffle(i1)
      for (dsi <- ir) {
        if (isRedundant(dsi)) {
          i1 -= dsi
          beta(dsi).foreach(w(_) -= 1)
        }
      }
    }
    def pruneInconsistent() = {
      var stop = false
      while (!stop) {
        val ir = rnd.shuffle(i1)
        val redundant = ir.filter(isRedundant(_))
        if (redundant.isEmpty)
          stop = true
        else {
          val dsi = redundant.minBy(i => consistencyWith(i) * diversityWith(iteration, i, candidates(i).coveredEntities))
          beta(dsi).foreach(w(_) -= 1)
          i1 -= dsi
        }
      }
    }
    def sizeOk(i:Int, th:Double) =
      (beta(i).size / numEntities.toDouble) < th

    def pruneTh(U: mutable.BitSet) = {
      var stop = false
      while (!stop) {
        val freeSize = U.size
        val completeness = 1.0 - (freeSize / numEntities.toDouble)
        val th = completeness - thCov
        if ((avgConsistency(i1) < thCons) && (th > 0.0)) { // below thCons, try to prune further
          val pruneCandidates = i1.filter(sizeOk(_, th))
          if (!pruneCandidates.isEmpty) {
            val dsi = pruneCandidates.minBy(i => consistencyWith(i) * diversityWith(iteration, i, candidates(i).coveredEntities))
            beta(dsi).foreach(w(_) -= 1)
            i1 -= dsi
            U ++= _entities.filter(w(_) == 0)
          }
          else
            stop = true
        }
        else
          stop = true
      }
    }


    def isRedundant(i:Int) = beta(i).forall(w(_) >= 2)
    def consistencyWith(c:Int):Double =
      if (i1.size > 0)
        (i1.foldLeft(0.0)((sum, g) => sum + cMat(c)(g)) / i1.size)
      else
        0.0


    val U = mutable.BitSet() ++ _entities.filter(w(_) == 0) // _entities uncovered by this i1
    // coverUncoveredLocal(U)
    // coverUncoveredConsistent(U)
    coverUncoveredNewTh(U)
    pruneInconsistent()
    pruneTh(U)
    // pruneWorstFirst()
    // pruneRandomized()
  }

  def fusion(i1:Cover, i2:Cover) = {
    // println(s"fusing of ${i1.datasetIndices} vs ${i2.datasetIndices}")
    val both = commonalities(i1, i2)
    // println(s"commonalities: $both")
    val result = mutable.ArrayBuffer[Int]()
    for (b <- both)
      result += b

    val onlyOne = xor(i1, i2)
    val p1 = score(i1) / (score(i1) + score(i2))
    val p2 = 1.0 - p1
    for (i <- onlyOne) {
      val pInherit = if (i1.datasetIndices.contains(i)) p1 else p2
      if (rnd.nextDouble < pInherit)
        result += i
    }

    // println(s"result: ${result}")
    result
    // toCover(result.map(candidates(_)))
  }

  def consistentFusion(i1:Cover, i2:Cover) = {
    def consistencyGain(result:Seq[Int], cons:Double, c:Int): Double =
      if (result.size > 0)
        (result.foldLeft(0.0)((sum, g) => sum + cMat(c)(g)) / result.size) - cons
      else
        0.0

    val both = commonalities(i1, i2)
    val result = mutable.ArrayBuffer[Int]()
    for (b <- both)
      result += b

    var sum = 0.0
    var cnt = 0
    for (i <- result; j <- result; if j > i) {
        sum += cMat(i)(j)
        cnt += 1
    }
    val cons = sum / cnt.toDouble

    val onlyOne = xor(i1, i2)
    val tmp = mutable.BitSet()
    val p1 = score(i1) / (score(i1) + score(i2))
    val p2 = 1.0 - p1
    for (i <- onlyOne) {
      val pInherit = if (i1.datasetIndices.contains(i)) p1 else p2
      val pInheritCons = pInherit + 2 * consistencyGain(result, cons, i)
      if (rnd.nextDouble < pInheritCons)
        tmp += i
    }

    for (x <- tmp)
      result += x
    result
  }


  def mutate(buf:mutable.Buffer[Int], numMutations:Int = 1):Unit = {
    // val buf = mutable.ArrayBuffer[Int]() ++ i.datasetIndices
    for (_ <- 0 until numMutations) {
      val pos = rnd.nextInt(numCandidates)
      if (buf.contains(pos))
        buf -= pos
      else
        buf += pos
    }
  }

  def commonalities(i1:Cover, i2:Cover) =
    i1.datasetIndices.intersect(i2.datasetIndices)

  /* xor, but try to keep the relative positions, thats why not just union(diff(a,b),diff(b,a))*/
  def xor(i1:Cover, i2:Cover) = {
      val a = i1.datasetIndices diff i2.datasetIndices
      val b = i2.datasetIndices diff i1.datasetIndices
      val n = min(a.size, b.size)
      val result = new mutable.ArrayBuffer[Int](a.size+b.size)
      for (i <- 0 until n) {
        result += a(i)
        result += b(i)
      }
      if (a.size > b.size)
        result ++= a.takeRight(a.size-n)
      else
        result ++= b.takeRight(b.size-n)
      // println("calculated diff between "+i1.datasetIndices + " and "+i2.datasetIndices + " as "+result)
      result
  }

  def distMat() = {
    val n = population.size
    val mat = unitMatrix(n)
    for (i <- mat.indices) {
      val i1 = population(i)
      for (j <- i+1 until n) {
        val i2 = population(j)
        val dist = distance(i1, i2)
        mat(i)(j) = dist
        mat(j)(i) = dist
      }
    }
    mat
  }

  def updateDistVec(i:Int):Unit = {
    val n = population.size
    val i1 = population(i)
    for (j <- 0 until n; if i != j) {
      val i2 = population(j)
      val dist = distance(i1, i2)
      distances(i)(j) = dist
      distances(j)(i) = dist
    }
  }
}

abstract class InitialPopulationSetCoverer(k:Int, _entities: Seq[Int], _candidates: Seq[DrilledDataset],
  _thCons:Double = 0.0, _thCov:Double = 0.8) extends GreedySetCoverer(k, _entities, _candidates, _thCons, _thCov) {

  this: ResultScorer with ScoreWeighter with Picker =>

  override def name = "InitialPopulationSetCoverer"
  override def toString = s"$name with $resultScorerName, $distanceMeasureName, $PickerName"

  val unusedCandidates = mutable.Buffer() ++ candidateIndices

  override def covers(): Seq[Cover] = {
    val covers:Seq[Cover] = createCovers(0, List(), 0)
    covers.map(pruneInconsistent).distinct
  }

  protected def createCovers(iteration: Int, covers: List[Cover], lastChange:Int): Seq[Cover] = {
    // stopping condition
    val numUnused = unusedCandidates.size
    if ((numUnused == 0 && covers.size >= k) || lastChange == 10) {
      return covers.reverse.toSeq
    }

    // create new cover, record used datasets
    val nc = newCover(iteration)
    updateUsages(nc)

    val change = unusedCandidates.size < numUnused

    // add to solution set (if new), recurse
    return if (!covers.contains(nc)) {
        createCovers(iteration+1, nc :: covers, if (change) 0 else lastChange+1)
    }
      else {
        createCovers(iteration+1, covers, if (change) 0 else lastChange+1)
      }
  }

  override protected def updateUsages(newCover:Cover): Unit =
    for ((dsi, picks) <- newCover.datasetIndices.zip(newCover.picks)) {
      val covered = picks.map(_.forIndex)
      covered.foreach(usages(_)(dsi) += 1)

      // also update per-candidate usage array
      unusedCandidates -= dsi
    }
}