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
import rea.util.IdentityByContentHashFilter
import rea.analysis.DataTypes
import rea.analysis._

case class MappingResult(tColIdx: Int, cColIdx: Int, covA: Double, covB: Double, monog: Double, mat: SimMatrix)

class REA(index: Index, req: REARequest) {

  val MAX_CANDS = 10
  val datasetFilter = new IdentityByContentHashFilter()
  val weighter = new DFWeighter(index)
  val attribute = req.attribute.map(analyze)
  val entities = req.entities.map(analyze)
  val concept = req.concept.map(analyze)
  val preparedEntities = entities.map(new PreparedString(_, weighter))
  val targetIndices = entities.indices.toSet[Int]
  val matchTools = new MatchTools(weighter)
  val NULL_LOOKUP_LIMIT = 1
  val TOTAL_LOOKUP_LIMIT = 1
  var globalIdCounter = 0

  def process(): Seq[DrilledDataset] = {
    val relationalResults = processSearchResult(
      index.search(attribute, entities, concept, req.numResults))

    val entityResults = if (req.entityTables)
      entities.toSeq.zipWithIndex.flatMap {
        case (e, i) => processEntitySearchResult(
          index.searchSingle(attribute, e, concept, req.numResults / 5), i)
      }
      else
        Seq()

    val result = relationalResults ++ entityResults
    if (result.isEmpty)
      Seq()
    else
      DrilledDataset.normalize(relationalResults ++ entityResults)
  }

  def refreeEntities(result: Iterable[DrilledDataset]): Seq[(String, Int)] = {
    val entityResultCounts = result.flatMap(_.values).groupBy(_.forIndex).toList.filter(_._2.size > 0)
    val sd = new StandardDeviation()
    var sum = 0
    for ((idx, lst) <- entityResultCounts) {
      sd.increment(lst.size)
      sum += lst.size
    }
    val mean = sum / entityResultCounts.size.toDouble
    val stdDev = sd.getResult
    return entityResultCounts.filter { case (idx, lst) => lst.size < mean - stdDev }.map(_._1)
      .map(v => (entities(v), v))

  }

  def extractDomain(url: String): String = {
    new URL(url).getHost()
  }

  @tailrec
  final def processEntitySearchResult(searchResult: Option[SearchResult], forIdx: Int,
    result: List[DrilledDataset] = List(),
    nullLookups: Int = 0, totalLookups: Int = 0): Iterable[DrilledDataset] = {
    searchResult match {
      case None =>
        result
      case Some(SearchResult(datasets, handle)) => {
        val newResults = datasets.filterNot(datasetFilter.filter(_)).flatMap(processEntityDataset(_, forIdx))

        if (newResults.isEmpty && totalLookups < NULL_LOOKUP_LIMIT) {
          val nextSearchResult = index.continueSearch(handle)
          processEntitySearchResult(nextSearchResult, forIdx, result ++ newResults,
            if (!newResults.isEmpty) nullLookups else nullLookups + 1, totalLookups + 1)
        } else
          result ++ newResults
      }
    }
  }

  def processEntityDataset(dataset: Dataset, forIdx: Int): Iterable[DrilledDataset] = {
    val columns = dataset.relation.map(_.map(analyze))
    val attributes = dataset.attributes.map(analyze)
    val entity = req.entities(forIdx)

    val eH = bestLocation(entity, columns)
    val aM = locations(attribute, columns)

    if (eH.isEmpty)
      aM.flatMap(m => {
        // found attribute in first col
        if (m.a == 0)
          if (m.b == 0) // TODO: ugly special-casing
            processCellRange(entity, m, forIdx, dataset, columns.map(_(m.b)), null, row_idx = Some(m.b))
          else
            processCellRange(entity, m, forIdx, dataset, columns.map(_(m.b)), columns.map(_(0)), row_idx = Some(m.b))

        // found attribute in first row
        else if (m.b == 0)
          processCellRange(entity, m, forIdx, dataset, columns(m.a), columns(0), col_idx = Some(m.a))
        else
          None
      })
    else
      aM.flatMap(intersect(columns, eH.get, _)).map(intrs =>
        DrilledDataset(req, dataset,
          Array(DrilledValue(forIdx, intrs.v, dataset, intrs.aMatch.a, intrs.aMatch.b, intrs.x, intrs.y, intrs.sim, nextGlobalId())),
          entityDSScore(intrs.sim, eH.get.c, dataset),
          MatchInfo(intrs.aMatch.a, intrs.aMatch.b, null)))
  }

  def processCellRange(entity:String, m: MatchingIndices, forIdx: Int, dataset: Dataset, cells: Seq[String],
     contextCol: Array[String], col_idx: Option[Int] = None, row_idx: Option[Int] = None): Iterable[DrilledDataset] =
    if (cells.size > 5)
      None
    else
      cells.map(coerceToNumber).zipWithIndex.dropWhile(_._1.isEmpty).takeWhile(_._1.isDefined).map {
        case (d, i) =>
          DrilledDataset(req,
            dataset,
            Array(DrilledValue(forIdx, d.get, dataset, m.a, m.b, col_idx.getOrElse(i), row_idx.getOrElse(i), m.c, nextGlobalId())),
            entityDSScore(m.c, titleMatch(entity, dataset.title), dataset),
            MatchInfo(
              m.a, m.b,
              if (contextCol !=null) contextCol(i) else null))
      }

  def titleMatch(e:String, title:String) = matchTools.aFocusedByWordLevenshtein.similarity(e, title)

  def entityDSScore(attSim: Double, entitySim: Double, dataset: Dataset) =
    DrillScores.create(
      attSim,
      entitySim,
      0.0,
      Scorer.termScore(req, dataset),
      Scorer.titleScore(req, dataset),
      1.0 / entities.length,
      1.0,
      Scorer.domainScore(dataset)
      )

  def locations(x: Array[String], relation: Array[Array[String]]) = {
    val hm = matchTools.locate(x, relation)
    hm.selectThreshold(0.50f)
    hm.selectMaxDelta()
    hm.getMatchingIndices
  }

  def bestLocation(x: String, relation: Array[Array[String]]) = {
    val bestHit = matchTools.locate(x, relation).getBestHit
    if (bestHit.c > 0.75)
      Some(bestHit)
    else
      None
  }

  case class Intersection(x: Int, y: Int, sim: Double, v: Double, eMatch: MatchingIndices, aMatch: MatchingIndices)

  def intersect(relation: Array[Array[String]], e: MatchingIndices, a: MatchingIndices): Option[Intersection] = {
    if (e.a == a.a || e.b == a.b)
      return None
    val x = max(e.a, a.a)
    val y = max(e.b, a.b)
    val v = coerceToNumber(relation(x)(y))
    val sim = (e.c + a.c) / 2.0
    if (v.isEmpty)
      None
    else
      Some(Intersection(x, y, sim, v.get.asInstanceOf[java.lang.Double], e, a))
  }

  def processSearchResult(searchResult: Option[SearchResult],
    result: List[DrilledDataset] = List(),
    nullLookups: Int = 0, totalLookups: Int = 0, foundIndices: Set[Int] = Set()): Seq[DrilledDataset] =
    searchResult match {
      case None => result
      case Some(SearchResult(datasets, _)) => {
        // main processing
        val newCandidates = datasets.filterNot(datasetFilter.filter(_)).flatMap(processDataset)

        // preparing next iteration if necessary
        val newFoundIndices = markFoundIndicesHolistic(newCandidates, foundIndices)
        val allFoundIndices = foundIndices ++ newFoundIndices
        val foundNew = newFoundIndices.size > foundIndices.size

        // if there not enough results, try to continue with the next datasets from the index
        if (newFoundIndices.size >= targetIndices.size ||
          nullLookups >= NULL_LOOKUP_LIMIT ||
          totalLookups >= TOTAL_LOOKUP_LIMIT)
          return result ++ newCandidates

        val freeEntities = entities.zipWithIndex.collect {
          case (v, i) if !allFoundIndices.contains(i) => v
        }
        val nextSearchResult = index.search(attribute, freeEntities, concept, req.numResults)

        processSearchResult(nextSearchResult, result ++ newCandidates,
          if (foundNew) nullLookups else nullLookups + 1,
          totalLookups + 1,
          allFoundIndices)
      }
    }

  def markFoundIndicesHolistic(candidates: Iterable[DrilledDataset], alreadyFound: Set[Int] = Set()): Set[Int] =
    candidates.foldLeft(alreadyFound) {
      (resultSet, candidate) => resultSet ++ candidate.values.map(_.forIndex)
    }

  case class DatasetInProcess(dataset: Dataset,
    columns: Array[Array[String]],
    textColsIdx: Seq[Int],
    valueColMatches: Seq[(Int, Double)])
  // titleScore: Double)

  def processDataset(dataset: Dataset): Iterable[DrilledDataset] = {
    val candColumns = for (c <- dataset.relation) yield c.drop(1)
    val candHeader = dataset.attributes

    val colTypes = candColumns.map(DataTypes.columnType(_))
    val (candTextColsIdx, candNonTextColsIdx) = candColumns.indices.partition(colTypes(_) == STRING)
    val candNumHeader = candNonTextColsIdx.map(candHeader(_))

    if (candNumHeader.length == 0) {
      //            println(" ... no num cols!")
      return None
    }
    /* 3. Match attribute names to keywords -> identify value cols*/
    //    val keyword_syns = kwGroups.flatten.map(_.toLowerCase).toSet[String]
    // val keyword_syns = kwGroups.flatten.map(_.toLowerCase).map(Wordnet.synonyms).flatten.map(_.toLowerCase).toSet[String]
    val valueColMatches =
      (for ((c, sim) <- findValueCols(attribute, candNumHeader.toArray)) yield (candNonTextColsIdx(c), sim)).toList

    if (valueColMatches.isEmpty) {
      //            println(" ... no matching val cols!")
      return None
    }

    /* 4. Score context match (for now only title) */
    // val titleScore = findTitleScore(attribute, dataset)

    /* 5. try to find join columns, project results using identified value cols and join cols */
    val inProcessDataset = DatasetInProcess(dataset, candColumns, candTextColsIdx, valueColMatches) //, titleScore)
    val entityMatches = findEntityMatch(entities, inProcessDataset)
    val result: Iterable[DrilledDataset] = entityMatches match {
      /* filter datasets without match */
      case None => None
      /* filter datasets with to few matches */
      // case Some(mr: MappingResult) if (mr.covA < 0.05 || mr.covB < 0.05) => None
      case Some(mr: MappingResult) => {
        inProcessDataset.valueColMatches.flatMap {
          case (c, sim) => {
            // println("creating result to "+candHeader(c))
            processValueCol(
              entities.indices, inProcessDataset, mr, c, sim)
          }
        }
      }
    }
    result
  }

  def findEntityMatch(entities: Array[String], inProcessDataset: DatasetInProcess): Option[MappingResult] =
    matchTCol(0, preparedEntities, inProcessDataset)

  def processValueCol(tColIndices: Range, inProcessDataset: DatasetInProcess, mappingResult: MappingResult, vColIdx: Int, sim: Double) = {
    val valCol = inProcessDataset.columns(vColIdx)
    val mappingMap = mappingResult.mat.getMatchingIndicesMapAtoBWithSim()

    val newVals = tColIndices.flatMap(i =>
      if (mappingMap.isDefinedAt(i))
        coerceToNumber(valCol(mappingMap(i).b)) match {
          case Some(v) => Some(DrilledValue(i, v, inProcessDataset.dataset, mappingResult.cColIdx, mappingMap(i).b + 1, vColIdx, mappingMap(i).b + 1, mappingMap(i).c, nextGlobalId()))
          case None => None
        }
      else
        None)

    val averageAttributeSim = (inProcessDataset.valueColMatches.map(_._2).sum) / inProcessDataset.valueColMatches.size
    val dataset = inProcessDataset.dataset
    val scores = DrillScores.create(
      averageAttributeSim,
      //mappingResult.monog,
      mappingMap.toSeq.map(_._2.c).sum / mappingMap.size.toDouble,
      Scorer.conceptSim(req, dataset, mappingResult.cColIdx),
      Scorer.termScore(req, dataset),
      Scorer.titleScore(req, dataset),
      mappingResult.covA,
      mappingResult.covB,
      Scorer.domainScore(dataset)
      )

    if (newVals.isEmpty)
      None
    else {
      val minfo = MatchInfo(vColIdx, 0, null)
      // println("with minfo "+minfo)
      Some(DrilledDataset(req,
        dataset,
        newVals.toArray,
        scores,
        minfo))
    }
  }

  def findValueCols(keyword_syns: Array[String], candNumHeader: Array[String]) = {
    val cleanedNumHeader = candNumHeader.map(analyze)
    val mat = matchTools.doMatch(
      keyword_syns,
      // cleanedNumHeader, List(matchTools.ngram, matchTools.weightedByWordLevenshtein))
      cleanedNumHeader, List(matchTools.ngram, matchTools.aFocusedByWordLevenshtein))
    mat.selectThreshold(0.5)
    // mat.selectThreshold(0.3)
    mat.selectMaxDelta(0.1)

    val mapping = mat.getMatchingIndices
    val bestMatches = mutable.Map[Int, Double]()
    for (m <- mapping) {
      bestMatches(m.b) = max(bestMatches.getOrElse(m.b, 0.0), m.c)
    }
    for ((c, sim) <- bestMatches) yield (c, sim)
  }

  def matchTCol(tColIdx: Int, targetCol: Array[PreparedString], inProcessDataset: DatasetInProcess): Option[MappingResult] = {
    val mappings = inProcessDataset.textColsIdx.flatMap(
      cColIdx => matchCCol(tColIdx, targetCol, cColIdx,
        inProcessDataset.columns(cColIdx).map(cell => new PreparedString(analyze(cell), weighter)))
    )
    if (mappings.isEmpty)
      None
    else
      Some(mappings.maxBy(_.covA))
  }

  def matchCCol(tColIdx: Int, tCol: Array[PreparedString], cColIdx: Int, cCol: Array[PreparedString]): Option[MappingResult] = {
    val mat = matchTools.doMatch(tCol, cCol, List(matchTools.weightedByWordAndPositionLevenshtein))
    mat.selectThreshold(0.7)
    mat.selectBipartiteGreedy
    val covA = mat.getCoverageA()
    if (mat.getCoverageA == 0.0)
      return None
    val covB = mat.getCoverageB
    val monog = mat.getMonogamy
    Some(MappingResult(tColIdx, cColIdx, covA, covB, monog, mat))
  }

  def nextGlobalId() = {
    val id = globalIdCounter
    globalIdCounter += 1
    id
  }

}
