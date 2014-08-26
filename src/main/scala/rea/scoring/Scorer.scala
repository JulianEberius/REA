package rea.scoring

import scala.io.Source
import scala.math.{min, max}
import rea.definitions.Dataset
import rea.definitions.JSONSerializable
import rea.definitions.REARequest
import rea.knowledge.Domains
import rea.Config
import rea.util.jaccard
import rea.analysis.analyze
import de.tudresden.matchtools.MatchTools

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
    jaccard(req.termSet, ds.titleTermSet)

  def domainTermScore(req: REARequest, ds: Dataset) =
    jaccard(req.termSet, ds.domainInfo.termSet) // TODO

  def domainScore(ds: Dataset) =
    Domains.scoreOnly(ds)
}
