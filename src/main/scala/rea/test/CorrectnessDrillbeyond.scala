package rea.test

import scala.Array.canBuildFrom
import com.martiansoftware.jsap.{JSAP, JSAPResult, Switch, UnflaggedOption}
import rea.REA
import rea.definitions.Dataset
import rea.definitions.DrilledValue
import java.util.BitSet
import rea.definitions.REARequest
import rea.index.LocalIndex
import rea.index.RemoteIndex
import rea.definitions.DrilledDataset
import rea.coherence.Coherence
import rea.definitions.Cover
import com.martiansoftware.jsap.FlaggedOption
import rea.cover._
import rea.util.StopWatch
import rea.analysis.analyze
import java.io.File
import scala.io.StdIn.readLine
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.Calendar

object CorrectnessDrillBeyond extends REATest {

  var config:JSAPResult = null

  def main(args: Array[String]):Unit = {
    config = parseArgs(args)
    val entitiesRaw = getRawEntities(config.getString(ENTITIES_PATH))
    println("Input:\n" + entitiesRaw.mkString("\n"))

    val entities = entitiesRaw.map(analyze)
    val attribute = config.getString(ATTRIBUTE).split('|')
    val concept = config.getString(CONCEPT).split('|')
    val predicates = config.getString(PREDICATES).split('|')
    val index = if (config.getBoolean(LOCAL_INDEX))
      new LocalIndex(config.getString(INDEX_PATH), debug = config.getBoolean(DEBUG))
    else
      new RemoteIndex(config.getString(INDEX_PATH), debug = config.getBoolean(DEBUG))
    val useEntityTables = !config.getBoolean(NO_ENTITY_TABLES)

    var retrievalRunTime:Long = 0
    val req = new REARequest(concept, entities, attribute, config.getInt(NUM_INDEX_CANDIDATES), useEntityTables, predicates)
    val results = retrieveMatchResults(req) match {
      case Some(mr) => mr
      case None => {
        val rea = createREA(index, req)
        val startT = System.currentTimeMillis
        val r = rea.process()
        retrievalRunTime = System.currentTimeMillis - startT
        storeMatchResults(req, r)
        r
      }
    }
    if (results.size == 0) {
      println("No results.")
      return
    }

    val rankedResults = results.toSeq.sortBy(d => (d.values.size / entities.size.toDouble) * d.score).reverse
    val topResults = rankedResults
    val store = new CorrectnessStore(config.getString(ANSWERS_DB))
    val rStore = new ResultStoreDRB(config.getString(DATABASE_FILE))
    val attrString = config.getString(ATTRIBUTE)

    val coverableEntities = entities.indices.filter(
      ei => topResults.exists(
        r => r.coveredEntities(ei)) )

    val searchSpaceFactor = config.getInt(SEARCH_SPACE_FACTOR)
    var algos = List[SetCoverer]()
    // val combinations = Seq( (0.0, 1.0), (0.4, 0.9), (0.4,0.0) )
    val combinations = Seq( (0.0,1.0))
    val k = 5
    for ((th,tc) <- combinations) {
      // algos ::= new GreedySetCoverer(k, coverableEntities, topResults, thCons=th, thCov=tc)
      //     with ResultScorer
      //     with ScoreWeighter
      //     with InverseSimByEntityDistance
      //     with ExponentialDiversityPicker
      algos ::= new GeneticSetCovererQ(k, coverableEntities, topResults, searchSpaceFactor, thCons=th, thCov=tc)
          with ResultScorer
          with ScoreWeighter
          with InverseSimByEntityDistance
          with ExponentialDiversityPicker
          with GreedyResultSelector
    }

    for (coverer <- algos) {
      val startT = System.currentTimeMillis
      val covers = coverer.covers()
      val runTime = System.currentTimeMillis - startT
      val scores = doScoring(covers, coverer)
      val exId = rStore.storeExperiment(config.getString(ENTITIES_PATH), config.getInt(NUM_COVERS), entities.size,
        coverableEntities.size, covers.size, config.getString(ATTRIBUTE), config.getString(CONCEPT), config.getString(PREDICATES),
        coverer, runTime, retrievalRunTime, scores, collectOutput(), topResults.size, covers, entities)

      for (cover <- covers) {
        for ((ds, dsi) <- cover.datasets.zipWithIndex) {
            println(s"Concerning Dataset: ${Console.RED}${ds.dataset.title}${Console.RESET} (${ds.dataset.tableNum}) ${Console.MAGENTA}${ds.inherentScore}${Console.RESET} with matchedAttr ${Console.GREEN}${ds.matchedAttribute}${Console.RESET}: ${ds.dataset.domain} ${ds.dataset.url}")
            val v = cover.picks(dsi)
            var anyCorrect = false

            for (vv <- v) {
              val entity = entities(vv.forIndex)
              if (!store.answerExists(ds, attrString, entity)) {
                print("    "+vv.toString(entities) + "  ")
                var a1 = ""
                while (!List("y","n").contains(a1))
                    a1 = readLine()
                val correct = a1 == "y"
                if (correct)
                  anyCorrect = true
                store.storeAnswer(ds, attrString, entity, correct)
              } else
                println("exists in db")
            }
            println
            println("-------------------------------------------------")
            println
        }
      }
    }
    store.close()
    rStore.close()
  }
}

class ResultStoreDRB(dbFile:String) {
  Class.forName("org.sqlite.JDBC")
  var conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile)
  val stat = conn.createStatement()
  stat.executeUpdate("""create table if not exists experiments (
    id integer primary key,
    timestamp datetime default current_timestamp,
    inputFile text,
    numCovers integer,
    numEntities integer,
    numCoverableEntities integer,
    numCoversCreated integer,
    attribute text,
    concept text,
    method text,
    resultScorer text,
    distanceMeasure text,
    picker text,
    resultSelector text,
    runTime real,
    inherentScore real,
    consistency real,
    minSources real,
    coverage real,
    diversity real,
    coverQuality real,
    numResults integer,
    thCons real,
    thCov real,
    scale integer,
    retrievalRunTime real,
    predicates text);""")

  stat.executeUpdate("""create table if not exists covers (
    id integer,
    rank integer,
    inherentScore real,
    consistency real,
    minSources real,
    coverage real,
    coverQuality real,
    diversityUpTo real);""")

  stat.executeUpdate("""create table if not exists coverParts (
    id integer,
    rank integer,
    position integer,
    identifier text,
    entity text);""")

  val insertExperimentSQL = """insert into
    experiments(id, inputFile, numCovers, numEntities, numCoverableEntities, numCoversCreated, attribute, concept, method, resultScorer, distanceMeasure, Picker, resultSelector,
      runTime, inherentScore, consistency, minSources, coverage, diversity, coverQuality, numResults, thCons, thCov, scale, retrievalRunTime, predicates)
    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
  val insertExperiment = conn.prepareStatement(insertExperimentSQL)

  val experimentExistsSQL = """select count(*) from experiments
    where inputFile=? and numCovers=? and attribute=? and concept=? and method=? and resultScorer=? and distanceMeasure=? and picker=? and resultSelector=?
    and thCons=? and thCov=? and scale=? and predicates=?"""
  val experimentExistsQ = conn.prepareStatement(experimentExistsSQL)

  val insertCoverSQL = """insert into
    covers(id, rank, inherentScore, consistency, minSources, coverage, coverQuality, diversityUpTo)
    values (?, ?, ?, ?, ?, ?, ?, ?);"""
  val insertCover = conn.prepareStatement(insertCoverSQL)

  val insertCoverPartSQL = "insert into coverParts values (?, ?, ?, ?, ?);"
  val insertCoverPart = conn.prepareStatement(insertCoverPartSQL);

  def experimentExists(inputFile:String, numCovers: Int, attribute:String, concept:String, predicates:String, coverer:SetCoverer) = {
    experimentExistsQ.setString(1, inputFile)
    experimentExistsQ.setInt(2, numCovers)
    experimentExistsQ.setString(3, attribute)
    experimentExistsQ.setString(4, concept)
    experimentExistsQ.setString(5, coverer.name)
    experimentExistsQ.setString(6, Namer.nameResultSelector(coverer))
    experimentExistsQ.setString(7, coverer.distanceMeasureName)
    experimentExistsQ.setString(8, Namer.namePicker(coverer))
    experimentExistsQ.setString(9, Namer.nameResultSelector(coverer))
    coverer match {
      case gr:GreedySetCoverer => {
        experimentExistsQ.setDouble(10, gr.thCons)
        experimentExistsQ.setDouble(11, gr.thCov)
      }
      case _ => {
        experimentExistsQ.setNull(10, Types.DOUBLE)
        experimentExistsQ.setNull(11, Types.DOUBLE)
      }
    }
    coverer match {
      case gr:MultiGreedySetCoverer => {
        experimentExistsQ.setDouble(12, gr.searchSpaceMultiplier)
      }
      case gr:GeneticSetCoverer => {
        experimentExistsQ.setDouble(12, gr.searchSpaceMultiplier)
      }
      case _ => {
        experimentExistsQ.setNull(12, Types.DOUBLE)
      }
    }
    experimentExistsQ.setString(13, predicates)

    val result = experimentExistsQ.executeQuery()
    result.next()
    val count = result.getInt(1)
    count > 0
  }

  def storeExperiment(inputFile:String, numCovers: Int, numEntities:Int, numCoverable:Int, numCoversCreated:Int, attribute:String, concept:String, predicates:String,
    coverer:SetCoverer, runTime:Long, retrievalRunTime:Long, scores:ResultScores, output:String, numResults:Int, covers:Seq[Cover], entities:Seq[String]) = {
    insertExperiment.setString(2, inputFile)
    insertExperiment.setInt(3, numCovers)
    insertExperiment.setInt(4, numEntities)
    insertExperiment.setInt(5, numCoverable)
    insertExperiment.setInt(6, numCoversCreated)
    insertExperiment.setString(7, attribute)
    insertExperiment.setString(8, concept)
    insertExperiment.setString(9, coverer.name)
    insertExperiment.setString(10, coverer.resultScorerName)
    insertExperiment.setString(11, coverer.distanceMeasureName)
    insertExperiment.setString(12, Namer.namePicker(coverer))
    insertExperiment.setString(13, Namer.nameResultSelector(coverer))

    insertExperiment.setLong(14, runTime)
    insertExperiment.setDouble(15, scores.inherentScore)
    insertExperiment.setDouble(16, scores.consistency)
    insertExperiment.setDouble(17, scores.minSources)
    insertExperiment.setDouble(18, scores.coverage)
    insertExperiment.setDouble(19, scores.diversity)
    insertExperiment.setDouble(20, scores.coverQuality)
    insertExperiment.setInt(21, numResults)

    coverer match {
      case gr:GreedySetCoverer => {
        insertExperiment.setDouble(22, gr.thCons)
        insertExperiment.setDouble(23, gr.thCov)
      }
      case _ => {
        insertExperiment.setNull(22, Types.DOUBLE)
        insertExperiment.setNull(23, Types.DOUBLE)
      }
    }
    coverer match {
      case gr:MultiGreedySetCoverer => {
        insertExperiment.setDouble(24, gr.searchSpaceMultiplier)
      }
      case gr:GeneticSetCoverer => {
        insertExperiment.setDouble(24, gr.searchSpaceMultiplier)
      }
      case _ => {
        insertExperiment.setNull(24, Types.DOUBLE)
      }
    }
    insertExperiment.setLong(25, retrievalRunTime)
    insertExperiment.setString(26, predicates)

    insertExperiment.execute()

    val generatedKeys = insertExperiment.getGeneratedKeys()
    generatedKeys.next()
    val exId = generatedKeys.getLong(1)

    for ((c,rank) <- covers.zipWithIndex) {
      val entityIndices = mutable.BitSet() ++ coverer.entities
      insertCover.setLong(1, exId)
      insertCover.setInt(2, rank)
      insertCover.setDouble(3, c.inherentScore)
      insertCover.setDouble(4, c.consistency(coverer.cMat))
      insertCover.setDouble(5, c.minSources)
      insertCover.setDouble(6, c.coverage(coverer.numEntities))
      insertCover.setDouble(7, c.coverQuality(coverer.numEntities))
      insertCover.setDouble(8, Cover.diversity(covers.take(rank+1), coverer))
      insertCover.execute()
      for ((ds, pos) <- c.datasets.zipWithIndex) {
        for (v <- c.picks(pos)) {
          insertCoverPart.setLong(1, exId)
          insertCoverPart.setInt(2, rank)
          insertCoverPart.setInt(3, pos)
          insertCoverPart.setString(4, ds.dataset.signature)
          insertCoverPart.setString(5, entities(v.forIndex))
          insertCoverPart.execute()
          entityIndices -= v.forIndex
        }
      }
      for (ei <- entityIndices) {
        insertCoverPart.setLong(1, exId)
        insertCoverPart.setInt(2, rank)
        insertCoverPart.setNull(3, Types.INTEGER)
        insertCoverPart.setNull(4, Types.VARCHAR)
        insertCoverPart.setString(5, entities(ei))
        insertCoverPart.execute()
      }
    }
  }


  def close() = conn.close()
}
