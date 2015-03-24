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
import rea.util.{ IsNumericPredicate, NumericValuedPredicate, NumericPredicate }
import de.tudresden.matchtools.weights.Weighter
import rea.util.IdentityByContentHashFilter
import webreduce.typing.DataType

case class MappingResult(tColIdx: Int, cColIdx: Int, covA: Double, covB: Double, monog: Double, mat: SimMatrix)

class REA(index: Index, req: REARequest, _weighter: Weighter = null) {

  val MAX_CANDS = 10
  // val datasetFilter = new IdentityByContentHashFilter()
  val weighter =
    if (_weighter != null)
      _weighter
    else
      new DFWeighter(index)
  val attribute = req.attribute.map(analyze)
  val entities = req.entities.map(analyze)
  val concept = req.concept.map(analyze)
  val preparedEntities = entities.map(new PreparedString(_, weighter))
  val targetIndices = entities.indices.toSet[Int]
  val matchTools = new MatchTools(weighter)
  val NULL_LOOKUP_LIMIT = 1
  val TOTAL_LOOKUP_LIMIT = 1
  val RELATIONAL_CANDIDATES_MULTIPLIER = 10
  var globalIdCounter = 0

  def process(): Seq[DrilledDataset] = {
    val relationalResults = processSearchResult(
      // index.search(attribute, entities, concept, req.numResults*RELATIONAL_CANDIDATES_MULTIPLIER))
      index.search(attribute, entities, concept, req.numResults))

    val entityResults = if (req.entityTables)
      entities.toSeq.zipWithIndex.flatMap {
        case (e, i) => processEntitySearchResult(
          index.searchSingle(attribute, e, concept, max(req.numResults / 5, 1)), i)
      }
    else
      Seq()
    // println(s"req.numResults: ${req.numResults}")
    // println(s"relationalResults.size: ${relationalResults.size}")
    // println(s"entityResults.size: ${entityResults.size}")
    // println(s"index.search(attribute, entities, concept, req.numResults)): ${index.search(attribute, entities, concept, req.numResults).size}")

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
        // val newResults = datasets.filterNot(datasetFilter.filter(_)).flatMap(processEntityDataset(_, forIdx))
        val newResults = datasets.flatMap(processEntityDataset(_, forIdx))

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

    if (eH.isEmpty
      || !(aM.map(_.b).forall(_ > eH.get.b))) // hack hack: ignore entitiy hits if they appear below any of the attribute hits
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
      aM.flatMap(intersect(columns, eH.get, _)).map(intrs => {
        val drilledValues = Array(DrilledValue(forIdx, intrs.v, dataset, intrs.aMatch.a, intrs.aMatch.b, intrs.x, intrs.y, intrs.sim, nextGlobalId()))
        DrilledDataset(req, dataset,
          drilledValues,
          entityDSScore(intrs.sim, eH.get.c, dataset, drilledValues),
          MatchInfo(intrs.aMatch.a, intrs.aMatch.b, null))
      })
  }

  def _predicatesApply(cell: Object) =
    req.predicates.forall(_.applicableForValue(cell))

  def processCellRange(entity: String, m: MatchingIndices, forIdx: Int, dataset: Dataset, cells: Seq[String],
    contextCol: Array[String], col_idx: Option[Int] = None, row_idx: Option[Int] = None): Iterable[DrilledDataset] =
    if (cells.size > 5)
      None
    else
      // cells.map(coerceToNumber).zipWithIndex.drop(1).map {
      cells.map(coerceToNumber).zipWithIndex.drop(1).
        dropWhile { case (cell, _) => !req.predicates.isEmpty && !_predicatesApply(cell) }.
        takeWhile { case (cell, _) => _predicatesApply(cell) }.map {
          case (d, i) => {
            val drilledValues = Array(DrilledValue(forIdx, d, dataset, m.a, m.b, col_idx.getOrElse(i), row_idx.getOrElse(i), m.c, nextGlobalId()))
            DrilledDataset(req,
              dataset,
              drilledValues,
              entityDSScore(m.c, titleMatch(entity, dataset.title), dataset, drilledValues),
              MatchInfo(
                m.a, m.b,
                if (contextCol != null) contextCol(i) else null))
          }
        }

  def titleMatch(e: String, title: String) = matchTools.aFocusedByWordLevenshtein.similarity(e, title)

  def entityDSScore(attSim: Double, entitySim: Double, dataset: Dataset, values: Seq[DrilledValue]) =
    DrillScores.create(
      attSim,
      entitySim,
      0.0,
      Scorer.termScore(req, dataset),
      Scorer.titleScore(req, dataset),
      1.0 / entities.length,
      1.0,
      Scorer.domainScore(dataset),
      Scorer.predicateScore(req, values))

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

  case class Intersection(x: Int, y: Int, sim: Double, v: Object, eMatch: MatchingIndices, aMatch: MatchingIndices)

  def intersect(relation: Array[Array[String]], e: MatchingIndices, a: MatchingIndices): Option[Intersection] = {
    if (e.a == a.a || e.b == a.b)
      return None
    val x = max(e.a, a.a)
    val y = max(e.b, a.b)
    val v = coerceToNumber(relation(x)(y))
    if (_predicatesApply(v)) {
      val sim = (e.c + a.c) / 2.0
      Some(Intersection(x, y, sim, v, e, a))
    } else {
      None
    }

  }

  def processSearchResult(searchResult: Option[SearchResult],
    result: List[DrilledDataset] = List(),
    nullLookups: Int = 0, totalLookups: Int = 0, foundIndices: Set[Int] = Set()): Seq[DrilledDataset] =
    searchResult match {
      case None => result
      case Some(SearchResult(datasets, handle)) => {
        // for (d <- datasets) {
        //   println("CANDIDATES: " + d.title + " " + d.attributes.mkString("|"))
        // }

        val newCandidates = datasets.flatMap(processDataset)
        // for (d <- newCandidates) {
        //   println("MATCHES: " + d.dataset.title + " " + d.dataset.attributes.mkString("|"))
        // }

        // preparing next iteration if necessary
        val newFoundIndices = markFoundIndicesHolistic(newCandidates, foundIndices)
        val allFoundIndices = foundIndices ++ newFoundIndices
        val foundNew = newFoundIndices.size > foundIndices.size

        // if there not enough results, try to continue with the next datasets from the index
        if (allFoundIndices.size >= targetIndices.size ||
          nullLookups >= NULL_LOOKUP_LIMIT ||
          totalLookups >= TOTAL_LOOKUP_LIMIT)
          return result ++ newCandidates

        // NEW SEARCH WITH FREE ENTITIES
        // val freeEntities = entities.zipWithIndex.collect {
        //   case (v, i) if !allFoundIndices.contains(i) => v
        // }
        // println(s"freeEntities: ${freeEntities.mkString(" ")}")
        // val nextSearchResult = index.search(attribute, freeEntities, concept, req.numResults)

        // NEW SEARCH AS CONTINUED OLD SEARCH
        val nextSearchResult = index.continueSearch(handle)

        // WITH REFREEING
        // var freeEntitiesIdx = entities.zipWithIndex.collect {
        //   case (v, i) if !allFoundIndices.contains(i) => i
        // }.toSet[Int]
        // val refreedEntities = refreeEntities(newCandidates).map(_._2)
        // println(s"refreedEntities: ${refreedEntities}")
        // freeEntitiesIdx ++= refreedEntities
        // val freeEntities = freeEntitiesIdx.map(entities(_)).toArray
        // println(s"freeEntities: ${freeEntities.mkString(" ")}")
        // val nextSearchResult = index.search(attribute, freeEntities, concept, req.numResults)

        processSearchResult(nextSearchResult, result ++ newCandidates,
          if (foundNew) nullLookups else nullLookups + 1,
          totalLookups + 1,
          allFoundIndices)
      }
    }

  def markFoundIndicesHolistic(candidates: Iterable[DrilledDataset], alreadyFound: Set[Int] = Set()): Set[Int] =
    candidates.foldLeft(Set[Int]()) {
      (resultSet, candidate) =>
        resultSet ++ candidate.values.map(_.forIndex).filterNot(alreadyFound.contains(_))
    }

  case class ValueColMatch(objs: Seq[Object], originalIdx: Int, sim: Double)

  case class DatasetInProcess(dataset: Dataset,
    columns: Array[Array[String]],
    textColsIdx: Seq[Int],
    valueColMatches: Seq[ValueColMatch])
  // titleScore: Double)

  def processDataset(dataset: Dataset): Iterable[DrilledDataset] = {
    // println(dataset.title + " -> " + dataset.attributes.mkString("|"))
    val candColumns = for (c <- dataset.relation) yield c.drop(1)
    val candHeader = dataset.attributes

    // val colTypes = candColumns.map(DataTypes.columnType(_))
    // val (candTextColsIdx, candNonTextColsIdx) = candColumns.indices.partition(colTypes(_) == STRING)
    val (candTextColsIdx, candNonTextColsIdx) = candColumns.indices.partition(dataset.colTypes(_) == DataType.STRING)

    // special casing, but oh my
    val candValHeaderIdx =
      if (req.predicates.exists(_.isInstanceOf[NumericPredicate]))
        candNonTextColsIdx
      else
        candColumns.indices

    val valueCols = candValHeaderIdx.map(i => (candColumns(i), i)).map { case (col, i) => (col.map(coerceToNumber), i) }
    val numericValuedPredicates = req.predicates.filter(_.isInstanceOf[NumericValuedPredicate])
    val discValueCols =
      if (numericValuedPredicates.isEmpty)
        valueCols
      else
        valueCols.filter {
          case (vCol, vColIdx) =>
            numericValuedPredicates.exists(p => p.discriminates(vCol))
        }

    // println(s"there are ${candColumns.size} cols, ${candValHeaderIdx.size} numCols, and ${discValueCols.size} discriminating cols")
    val candValHeader = discValueCols.map { case (vCol, i) => candHeader(i) }
    // println(s"cand header: ${candValHeader.mkString(" ")}")

    if (candValHeader.length == 0) {
      return None
    }
    /* 3. Match attribute names to keywords -> identify value cols*/
    // val keyword_syns = kwGroups.flatten.map(_.toLowerCase).toSet[String]
    // val keyword_syns = kwGroups.flatten.map(_.toLowerCase).map(Wordnet.synonyms).flatten.map(_.toLowerCase).toSet[String]
    val valueColMatches =
      (for ((c, sim) <- findValueCols(attribute, candValHeader.toArray)) yield ValueColMatch(discValueCols(c)._1, discValueCols(c)._2, sim)).toList

    if (valueColMatches.isEmpty) {
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
      case Some(mr: MappingResult) => {
        inProcessDataset.valueColMatches.flatMap {
          case ValueColMatch(col, originalIdx, sim) => {
            processValueCol(
              col, entities.indices, inProcessDataset, mr, originalIdx, sim)
          }
        }
      }
    }
    result
  }

  def findEntityMatch(entities: Array[String], inProcessDataset: DatasetInProcess): Option[MappingResult] =
    matchTCol(0, preparedEntities, inProcessDataset)

  def processValueCol(valCol: Seq[Object], tColIndices: Range, inProcessDataset: DatasetInProcess, mappingResult: MappingResult, vColIdx: Int, sim: Double) = {
    val mappingMap = mappingResult.mat.getMatchingIndicesMapAtoBWithSim()

    val newVals = tColIndices.flatMap(i =>
      if (mappingMap.isDefinedAt(i))
        Some(DrilledValue(i, valCol(mappingMap(i).b), inProcessDataset.dataset, mappingResult.cColIdx, mappingMap(i).b + 1, vColIdx, mappingMap(i).b + 1, mappingMap(i).c, nextGlobalId()))
      else
        None)

    val averageAttributeSim = (inProcessDataset.valueColMatches.map(_.sim).sum) / inProcessDataset.valueColMatches.size
    val dataset = inProcessDataset.dataset
    val scores = DrillScores.create(
      averageAttributeSim,
      mappingMap.toSeq.map(_._2.c).sum / mappingMap.size.toDouble,
      Scorer.conceptSim(req, dataset, mappingResult.cColIdx),
      Scorer.termScore(req, dataset),
      Scorer.titleScore(req, dataset),
      mappingResult.covA,
      mappingResult.covB,
      Scorer.domainScore(dataset),
      Scorer.predicateScore(req, newVals))

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
      cleanedNumHeader, List(matchTools.ngram, matchTools.aFocusedByWordLevenshtein))
    mat.selectThreshold(0.5)
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
        inProcessDataset.columns(cColIdx).map(cell => new PreparedString(analyze(cell), weighter))))
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
