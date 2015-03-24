package rea.util

import rea.{REA, REA2}
import rea.index._
import rea.definitions._
import rea.util._
import rea.cover._
import rea.cover.SetCoverer.printCovers
import rea.analysis.analyze

class ScoredSelectivity(val score:Double, val selectivity:Double) extends JSONSerializable

object Selectivity {

    type estimatesFunc = (REARequest, Index) => Option[Seq[ScoredSelectivity]]

    def estimate(req:REARequest, index:Index): Option[Double] =
      estimateMedian(estimatesFull)(req, index)

    def estimates(req:REARequest, index:Index):  Option[Seq[ScoredSelectivity]] =
      estimatesFull(req, index)

    def estimateMedian(ef:estimatesFunc)(req:REARequest, index:Index): Option[Double] =
      ef(req, index) match {
        case None => None
        case Some(estimates) => {
          val sortedEstimates = estimates.sortBy(_.selectivity)
          println(s"SORTED ESTIMATES: ${sortedEstimates.map(_.selectivity)}")
          Some(sortedEstimates(estimates.size / 2).selectivity)
        }
      }

    def estimateAvg(ef:estimatesFunc)(req:REARequest, index:Index): Option[Double] =
      ef(req, index) match {
        case None => None
        case Some(estimates) => {
          val sum = estimates.foldLeft(0.0)( (agg, ss) => agg + ss.selectivity * ss.score)
          val sum_weights = estimates.foldLeft(0.0)( (agg, ss) => agg + ss.score)
          Some(sum / sum_weights)
        }
      }

    def estimatesFull(req:REARequest, index:Index): Option[Seq[ScoredSelectivity]] = {
        if (req.predicates.size != 1) {
          println("Selectivity estimates can only be computed for exactly one predicate. REARequest: "+req)
          return None
        }

        val rea = new REA2(index, req)
        println(s"[${fTime}]  Starting REA process")
        val reaResults = rea.process(req.numResults)
        println(s"[${fTime}]  Finished REA process")
        if (reaResults.size == 0) {
          println("No reaResults.")
          return None
        }

        val topResults = reaResults.toSeq.sortBy(_.scores.score).reverse
        println(s"${reaResults.size} reaResults, using " + topResults.size)

        val entities = req.entities.map(analyze)
        val coverableEntities = entities.indices.filter(
          ei => topResults.exists(
            r => r.coveredEntities(ei)))
        println(s"coverableEntities: ${coverableEntities.size}")

        val algo = new GreedySetCoverer(req.numResults, coverableEntities, topResults, 0.0, 1.0)
           with ResultScorer
           with ScoreWeighter
           with InverseSimDistance
           with ExponentialDiversityPicker

        println(s"[${fTime}]  Starting SetCoverer")
        val covers = algo.covers()
        // println("Covers for selectivity estimation: ")
        // printCovers(covers, algo, entities)
        println(s"[${fTime}]  Finished SetCoverer")

        println("PREDS USED: "+req.predicates(0) + " other preds "+req.predicates.drop(1))
        val selectivities = covers.map( c =>
          new ScoredSelectivity(
            c.inherentScore,
            req.predicates(0).selectivity(c.picks.flatten.map(_.value))))

        Some(selectivities)
    }

    def estimatesSimple(req:REARequest, index:Index): Option[Seq[ScoredSelectivity]] = {
        if (req.predicates.size != 1) {
          println("Selectivity estimates can only be computed for exactly one predicate. REARequest: "+req)
          return None
        }

        val rea = new REA2(index, req)
        val reaResults = rea.process()
        if (reaResults.size == 0) {
          println("No reaResults.")
          return None
        }

        val topResults = reaResults.toSeq.sortBy(_.scores.score).reverse
        println(s"${reaResults.size} reaResults, using " + topResults.size)
        val selectivities = topResults.map( r =>
          new ScoredSelectivity(
            r.scores.score,
            req.predicates(0).selectivity(r.values.map(_.value))))

        val entities = req.entities
        topResults.foreach(r => {
          println(r.dataset.title)
          println(s"isNumeric ${r.isNumeric}")
          println("predicateScore: "+r.scores.predicateScore)
          // println(r.dataset.prettyTable(5))
          println("Values:\n " + r.values.map( value =>
            s"Value for: ${Console.YELLOW}${entities(value.forIndex)}${Console.RESET}, value: ${value.value}, matched: ${Console.YELLOW}${r.dataset.relation(value.eColIdx)(value.eRowIdx)}${Console.RESET}, oValue: ${r.dataset.relation(value.colIdx)(value.rowIdx)}"
          ).mkString("\n"))
          println("Predicate Selectivity: " + req.predicates.map(_ match {
            case p:NoPredicate => "no predicate"
            case p:Predicate => p.selectivity(r.values.map(_.value)).toString
          }).mkString(" "))
          println(r.tags)
          println()
        })

        Some(selectivities)
    }

}