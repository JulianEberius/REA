package rea

import java.net.URL
import scala.Array.canBuildFrom
import scala.Array.fallbackCanBuildFrom
import scala.Option.option2Iterable
import scala.annotation.tailrec
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable
import scala.math.max
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation
import de.tudresden.matchtools.MatchTools
import de.tudresden.matchtools.SimMatrix
import de.tudresden.matchtools.datastructures.MatchingIndices
import de.tudresden.matchtools.datastructures.PreparedString
import rea.analysis.DataTypes.coerceToNumber
import rea.analysis.analyze
import rea.definitions.Dataset
import rea.definitions.DrilledDataset
import rea.definitions.DrilledValue
import rea.definitions.MatchInfo
import rea.definitions.REARequest
import rea.definitions.DrillScores
import rea.index.Index
import rea.index.RemoteIndex
import rea.index.SearchResult
import rea.scoring.Scorer
import rea.util.DFWeighter
import rea.util.{IsNumericPredicate, NumericValuedPredicate, fTime}
import de.tudresden.matchtools.weights.Weighter
import rea.util.IdentityByContentHashFilter
import webreduce.typing.DataType


class REA2(index: Index, req: REARequest, _weighter: Weighter = null) extends REA(index, req) {

  override def process(): Seq[DrilledDataset] = process(req.numResults)

  def process(numResults:Int): Seq[DrilledDataset] = {
    val sr = index.search2(attribute, entities, concept, numResults)
    // println(s"[${fTime}]  received Relational results")
    val relationalResults = processSearchResult(sr)
    // println(s"[${fTime}]  processed Relational results")

    val entityResults = if (req.entityTables)
      entities.toSeq.zipWithIndex.flatMap {
        case (e, i) => processEntitySearchResult(
          index.searchSingle2(attribute, e, concept, max(req.numResults / 5, 1)), i)
      }
    else
      Seq()
    // println(s"[${fTime}]  processed entity results")
    // println(s"req.numResults: ${req.numResults}")
    // println(s"relationalResults.size: ${relationalResults.size}")
    // println(s"entityResults.size: ${entityResults.size}")

    val result = relationalResults ++ entityResults
    if (result.isEmpty)
      Seq()
    else
      DrilledDataset.normalize(relationalResults ++ entityResults)
  }

  override def processSearchResult(searchResult: Option[SearchResult],
    result: List[DrilledDataset] = List(),
    nullLookups: Int = 0, totalLookups: Int = 0, foundIndices: Set[Int] = Set()): Seq[DrilledDataset] = searchResult match {
      case None => result
      case Some(SearchResult(datasets, handle)) => datasets.flatMap(processDataset)
    }

}
