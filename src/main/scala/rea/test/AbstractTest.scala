package rea.test

import scala.Array.canBuildFrom
import com.martiansoftware.jsap.{JSAP, JSAPResult, Switch, UnflaggedOption}
import rea.{REA, REA2}
import rea.definitions.Dataset
import rea.definitions.DrilledValue
import java.util.BitSet
import rea.definitions.REARequest
import rea.index.LocalIndex
import rea.index.Index
import rea.index.RemoteIndex
import rea.definitions.DrilledDataset
import rea.coherence.Coherence
import rea.definitions.Cover
import com.martiansoftware.jsap.FlaggedOption
import rea.cover._
import rea.util.{StopWatch, RedisConnector, unitMatrix}
import rea.Config
import rea.analysis.analyze
import java.io.File
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import java.sql.SQLException
import java.sql.Types
import scala.util.Random
import scala.io.Source
import scala.collection.immutable
import java.io._
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken

case class ResultScores(
  inherentScore:Double, consistency:Double,
  minSources:Double, coverage:Double,
  diversity:Double, scoreSingle:Double, coverQuality:Double)

trait REATest {
  val INDEX_PATH = "indexPath"
  val LOCAL_INDEX = "localIndex"
  val ENTITIES_PATH = "entitiesFile"
  val DEBUG = "debug"
  val ATTRIBUTE = "attribute"
  val CONCEPT = "concept"
  val PREDICATES = "predicates"
  val NO_ENTITY_TABLES = "noEntityTables"
  val NUM_COVERS = "numCovers"
  val WARMUP = "warmUp"
  val GUI = "gui"
  val OPTIMAL = "optimal"
  val PRINT_COVERS = "printCovers"
  val DATABASE_FILE = "dbFile"
  val NUM_INDEX_CANDIDATES = "numIndexCandidates"
  val SEARCH_SPACE_FACTOR = "searchSpaceFactor"
  val FILTERED_API = "filteredApi"
  val REA2 = "rea2"
  val ANSWERS_DB = "answersFile"


  var config:JSAPResult

  def createREA(index:Index, req:REARequest) =
    if (config.getBoolean(REA2))
        new REA2(index, req)
    else
        new REA(index, req)

  def getRawEntities(entitiesPath: String): Array[String] =
    if (entitiesPath == null)
      io.Source.stdin.getLines().toArray
    else if (new File(entitiesPath).exists())
      io.Source.fromFile(entitiesPath).getLines().toArray
    else
      entitiesPath.split("|")

  def printMatrix(mat: Array[Array[Double]]) {
    for (l <- mat)
      println(l.map("%.3f".format(_)).mkString(" "))
  }

  def matrixToString(mat: Array[Array[Double]]):String = {
    val sb = new StringBuilder
    for (l <- mat)
      sb ++= l.map("%.3f".format(_)).mkString(""," ","\n")
    sb.toString
  }

  def sharesdDsMat(covers: Seq[Cover]) = {
    val sharedDS = Array.fill(covers.size, covers.size)(0.0)
    for (x <- 0 until covers.size; y <- 0 until covers.size) {
      val a = covers(x).datasets.toSet
      val b = covers(y).datasets.toSet

      sharedDS(x)(y) = (a.intersect(b)).size.toDouble / a.union(b).size.toDouble
    }
    sharedDS
  }

  def parseArgs(args: Array[String]) = {
    val jsap = new JSAP()
    jsap.registerParameter(new FlaggedOption(INDEX_PATH).setLongFlag(INDEX_PATH)
      .setRequired(true).setShortFlag('i'))

    jsap.registerParameter(new Switch(LOCAL_INDEX)
      .setLongFlag(LOCAL_INDEX).setShortFlag('l'))

    jsap.registerParameter(new Switch(NO_ENTITY_TABLES)
      .setLongFlag(NO_ENTITY_TABLES).setShortFlag('t'))

    jsap.registerParameter(new Switch(WARMUP)
      .setLongFlag(WARMUP).setShortFlag('w'))

    jsap.registerParameter(new FlaggedOption(ENTITIES_PATH).setLongFlag(ENTITIES_PATH)
      .setRequired(false).setShortFlag('e'))

    jsap.registerParameter(new FlaggedOption(ATTRIBUTE).setLongFlag(ATTRIBUTE)
      .setRequired(false).setShortFlag('a').setDefault("revenue"))

    jsap.registerParameter(new FlaggedOption(CONCEPT).setLongFlag(CONCEPT)
      .setRequired(false).setShortFlag('c').setDefault("country"))

    jsap.registerParameter(new FlaggedOption(DATABASE_FILE).setLongFlag(DATABASE_FILE)
      .setRequired(false).setShortFlag('f').setDefault("results.db"))

    jsap.registerParameter(new FlaggedOption(ANSWERS_DB).setLongFlag(ANSWERS_DB)
      .setRequired(false).setShortFlag('j').setDefault("answers.db"))

    jsap.registerParameter(new FlaggedOption(PREDICATES).setLongFlag(PREDICATES)
      .setRequired(false).setShortFlag('r').setDefault(""))

    jsap.registerParameter(new FlaggedOption(NUM_COVERS).setLongFlag(NUM_COVERS)
      .setRequired(false).setShortFlag('n').setStringParser(JSAP.INTEGER_PARSER).setDefault("5"))

    jsap.registerParameter(new FlaggedOption(NUM_INDEX_CANDIDATES).setLongFlag(NUM_INDEX_CANDIDATES)
      .setRequired(false).setShortFlag('u').setStringParser(JSAP.INTEGER_PARSER).setDefault("100"))

    jsap.registerParameter(new FlaggedOption(SEARCH_SPACE_FACTOR).setLongFlag(SEARCH_SPACE_FACTOR)
      .setRequired(false).setShortFlag('s').setStringParser(JSAP.INTEGER_PARSER).setDefault("10"))

    jsap.registerParameter(new Switch(DEBUG)
      .setLongFlag(DEBUG).setShortFlag('d'))

    jsap.registerParameter(new Switch(FILTERED_API)
      .setLongFlag(FILTERED_API).setShortFlag('b'))

    jsap.registerParameter(new Switch(GUI)
      .setLongFlag(GUI).setShortFlag('g'))

    jsap.registerParameter(new Switch(OPTIMAL)
      .setLongFlag(OPTIMAL).setShortFlag('o'))

    jsap.registerParameter(new Switch(PRINT_COVERS)
      .setLongFlag(PRINT_COVERS).setShortFlag('p'))

    jsap.registerParameter(new Switch(REA2)
      .setLongFlag(REA2).setShortFlag('2'))

    jsap.parse(args)
  }

  var sb:StringBuilder = new StringBuilder
  def out() = {
    sb.append("\n")
    println
  }

  def out(d:Double) = {
    sb.append(d.toString)
    sb.append("\n")
    println(d)
  }

  def out(s:String) = {
    sb.append(s)
    sb.append("\n")
    println(s)
  }

  def collectOutput():String = {
    val s = sb.toString
    sb = new StringBuilder
    s
  }


  def differenceString(a:Double, b:Double) =
    if (a >= b)
      f"+${a-b}%.2f"
    else
      f"${a-b}%.2f"

  def printCovers(covers:Seq[Cover], coverer: SetCoverer, entities: Array[String]) = {
    if (config.getBoolean(PRINT_COVERS)) {
      for ((c, i) <- covers.zipWithIndex) {
        out(s"$i)\n--")
        out(c.toString(entities))
        out("cov: " + c.coverage(coverer.numEntities))
        out("score: " + c.inherentScore)
        out("cons: " + c.consistency(coverer.cMat))
        // out(matrixToString(consistencyMatrix(c, coverer)))
        // out(c.datasets.map(d => d.scores.toString + "\n" +
        //   s"(${d.scores.attSim} + ${d.scores.entitySim} * 0.9 + ${d.scores.conceptSim} * 0.25 + ${d.scores.termSim} * 0.25 + ${d.scores.titleSim} * 0.25 + ${d.scores.coverageA} + ${d.scores.coverageB} * 0.5  + ${d.scores.domainPopularity} * 0.8)"+
        //   "\n" + s"${d.scores.score} + ${d.scores.inherentScore}").mkString("\n"))
        // out(c.datasets.map(d => d.scores.toString).mkString("\n"))
        out
      }
    }
  }

  /* simple time measurement */
  private var t:Long = 0
  def startTimer(): Unit = {
    t = System.currentTimeMillis
  }
  def stopTimer(): Long =
    System.currentTimeMillis - t

  def doScoring(covers:Seq[Cover], coverer:SetCoverer) = {
    val inherentScore = Cover.inherentScore(covers)
    val consistency = Cover.consistency(covers, coverer)
    val minSources = Cover.minSources(covers)
    val coverage = Cover.coverage(covers, coverer.entities.size)
    val diversity = Cover.diversity(covers, coverer)
    val scoreSingle = Cover.score(covers, coverer)
    val coverQuality = Cover.coverQuality(covers, coverer.entities.size)
    ResultScores(inherentScore, consistency, minSources, coverage, diversity, scoreSingle, coverQuality)
  }
  def printScores(refScores:ResultScores, scores:ResultScores) = {
    out("Coverage: " + differenceString(scores.coverage, refScores.coverage))
    out("InherentScore: " + differenceString(scores.inherentScore, refScores.inherentScore))
    out("Consistency: " + differenceString(scores.consistency, refScores.consistency))
    out("MinSources: " + differenceString(scores.minSources, refScores.minSources))
    out("Diversity: " + differenceString(scores.diversity, refScores.diversity))
    out("ScoreSingle: " + differenceString(scores.scoreSingle, refScores.scoreSingle))
    out("CoverQuality: " + differenceString(scores.coverQuality, refScores.coverQuality))
    out
  }
  def printScores(scores:ResultScores) = {
    out("Coverage: " + f"${scores.coverage}%.2f")
    out("InherentScore: " + f"${scores.inherentScore}%.2f")
    out("Consistency: " + f"${scores.consistency}%.2f")
    out("MinSources: " + f"${scores.minSources}%.2f")
    out("Diversity: " + f"${scores.diversity}%.2f")
    out("ScoreSingle: " + f"${scores.scoreSingle}%.2f")
    out("CoverQuality: " + f"${scores.coverQuality}%.2f")
    out
    out
  }

  val gson = new Gson

  def matchResultStorageKey(req:REARequest):String =
    req.entityTables match {
      case true => s"""${req.concept.mkString("|")}-${req.attribute.mkString("|")}-${req.entities.mkString("|")}-${req.numResults}-${req.predicates}"""
      case false => s"""${req.concept.mkString("|")}-${req.attribute.mkString("|")}-${req.entities.mkString("|")}-${req.numResults}-${req.predicates}-noEntityTables"""
    }

  def storeMatchResults(req:REARequest, results:Seq[DrilledDataset]) = {
    val resultJson = gson.toJson(results.asJava)
    val key = matchResultStorageKey(req)

    val jedis = RedisConnector.getConn(Config.REDIS_MATCH_RESULT_DB)
    jedis.set(key, resultJson)
    RedisConnector.release(jedis)
  }

  def retrieveMatchResults(req:REARequest) = {
    val key = matchResultStorageKey(req)
    val jedis = RedisConnector.getConn(Config.REDIS_MATCH_RESULT_DB)
    val jsonString = jedis.get(key)
    RedisConnector.release(jedis)
    if (jsonString != null) {
      val DDS = gson.fromJson(jsonString, classOf[Array[DrilledDataset]]).toSeq

      for (dds <- DDS) {
        dds.req = req
        for (v <- dds.values) {
          v.dataset = dds.dataset
        }
      }
      Some(DDS)
    }
    else
      None
  }

  def testAlgo(sc:SetCoverer, entities:Array[String], refScores: ResultScores = null) = {
    val covers = sc.covers()

    val scores = doScoring(covers, sc)
    printCovers(covers, sc, entities)
    if (refScores != null)
      printScores(refScores, scores)
    else
      printScores(scores)

    (covers, scores)
  }

  def distanceMatrix(covers:Seq[Cover], coverer:SetCoverer) = {
    val matrix = unitMatrix(covers.size)
    val size = covers.size
    var i = 0
    var j = 0
    while (i < size) {
      j = 0
      while (j < size) {
        if (j > i) {
          val c = coverer.distance(covers(i), covers(j))
          matrix(i)(j) = c
          matrix(j)(i) = c
        }
        j += 1
      }
      i +=1
    }
    matrix
  }

  def consistencyMatrix(cover:Cover, coverer:SetCoverer) = {
    val size = cover.datasetIndices.size
    val matrix = unitMatrix(size)
    var i = 0
    var j = 0
    while (i < size) {
      j = 0
      while (j < size) {
        if (j > i) {
          val c = coverer.cMat(cover.datasetIndices(i))(cover.datasetIndices(j))
          matrix(i)(j) = c
          matrix(j)(i) = c
        }
        j += 1
      }
      i +=1
    }
    matrix
  }



}

class ResultStore(dbFile:String) {
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
    retrievalRunTime real);""")

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
      runTime, inherentScore, consistency, minSources, coverage, diversity, coverQuality, numResults, thCons, thCov, scale, retrievalRunTime)
    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
  val insertExperiment = conn.prepareStatement(insertExperimentSQL)

  val experimentExistsSQL = """select count(*) from experiments
    where inputFile=? and numCovers=? and attribute=? and concept=? and method=? and resultScorer=? and distanceMeasure=? and picker=? and resultSelector=?
    and thCons=? and thCov=? and scale=?"""
  val experimentExistsQ = conn.prepareStatement(experimentExistsSQL)

  // val insertOutputSQL = "insert into outputs values (?, ?);"
  // val insertOutput = conn.prepareStatement(insertOutputSQL);

  val insertCoverSQL = """insert into
    covers(id, rank, inherentScore, consistency, minSources, coverage, coverQuality, diversityUpTo)
    values (?, ?, ?, ?, ?, ?, ?, ?);"""
  val insertCover = conn.prepareStatement(insertCoverSQL)

  val insertCoverPartSQL = "insert into coverParts values (?, ?, ?, ?, ?);"
  val insertCoverPart = conn.prepareStatement(insertCoverPartSQL);

  def experimentExists(inputFile:String, numCovers: Int, attribute:String, concept:String, coverer:SetCoverer) = {
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

    val result = experimentExistsQ.executeQuery()
    result.next()
    val count = result.getInt(1)
    count > 0
  }

  def storeExperiment(inputFile:String, numCovers: Int, numEntities:Int, numCoverable:Int, numCoversCreated:Int, attribute:String, concept:String,
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

    insertExperiment.execute()

    val generatedKeys = insertExperiment.getGeneratedKeys()
    generatedKeys.next()
    val exId = generatedKeys.getLong(1)

    // insertOutput.setLong(1, exId)
    // insertOutput.setString(2, output)
    // insertOutput.execute()

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

class CorrectnessStore(dbFile:String) {
  Class.forName("org.sqlite.JDBC")
  var conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile)
  val stat = conn.createStatement()
  stat.executeUpdate("""create table if not exists answers (
    identifier text,
    forAttribute text,
    entity text,
    correct integer);""")
  stat.executeUpdate("""create table if not exists tags (
    identifier text,
    forAttribute text,
    tag text);""")

  val insertAnswerSQL = """insert into
    answers(identifier, forAttribute, entity, correct)
    values (?, ?, ?, ?);"""
  val insertAnswer = conn.prepareStatement(insertAnswerSQL)

  val insertTagSQL = "insert into tags values (?, ?, ?);"
  val insertTag = conn.prepareStatement(insertTagSQL)

  val answerExists = conn.prepareStatement("select count(*) from answers where identifier = ? and forAttribute = ? and entity = ?;")

  def answerExists(ds:DrilledDataset, attr:String, entity:String): Boolean = {
    answerExists.setString(1, ds.dataset.signature)
    answerExists.setString(2, attr)
    answerExists.setString(3, entity)
    val result = answerExists.executeQuery()

    result.next()
    val count = result.getInt(1)
    count > 0
  }

  val tagsExist = conn.prepareStatement("select count(*) from tags where identifier = ? and forAttribute = ?;")

  def tagsExist(ds:DrilledDataset, attr:String): Boolean = {
    tagsExist.setString(1, ds.dataset.signature)
    tagsExist.setString(2, attr)
    val result = tagsExist.executeQuery()

    result.next()
    val count = result.getInt(1)
    count > 0
  }

  val domainTagsExist = conn.prepareStatement("select count(*) from tags where identifier = ? and forAttribute = ? and tag like 'domain:%';")

  def domainTagsExist(ds:DrilledDataset, attr:String): Boolean = {
    domainTagsExist.setString(1, ds.dataset.signature)
    domainTagsExist.setString(2, attr)
    val result = domainTagsExist.executeQuery()

    result.next()
    val count = result.getInt(1)
    count > 0
  }

  def addDomainTag(ds:DrilledDataset, attr:String) = {
    insertTag.setString(1, ds.dataset.signature)
    insertTag.setString(2, attr)
    insertTag.setString(3, s"domain:${ds.dataset.domain}")
    insertTag.execute()
  }

  def storeAnswer(ds:DrilledDataset, attr:String, entity:String, correct:Boolean) = {
    insertAnswer.setString(1, ds.dataset.signature)
    insertAnswer.setString(2, attr)
    insertAnswer.setString(3, entity)
    insertAnswer.setBoolean(4, correct)
    insertAnswer.execute()
  }
  def storeTags(ds:DrilledDataset, attr:String, tags:Set[String]) = {
    for (t <- tags) {
      insertTag.setString(1, ds.dataset.signature)
      insertTag.setString(2, attr)
      insertTag.setString(3, t)
      insertTag.execute()
    }
  }

  def close() = conn.close()
}

object Namer {
  def namePicker(coverer:SetCoverer):String = coverer match {
    case c:Picker => c.PickerName
    case _ => null
  }
  def nameResultSelector(coverer:SetCoverer):String = coverer match {
    case c:ResultSelector => c.resultSelectorName
    case _ => null
  }
}
