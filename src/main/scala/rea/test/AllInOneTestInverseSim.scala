package rea.test

import scala.Array.canBuildFrom
import com.martiansoftware.jsap.JSAPResult
import rea.REA
import rea.definitions._
import rea.definitions.REARequest
import rea.index.{LocalIndex, RemoteIndex, Index}
import rea.cover._
import rea.analysis.analyze
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object AllInOneTestInverseSim extends REATest {

  var config:JSAPResult = null
  var index:Index = null
  val useEntityTables = true

  def main(args: Array[String]):Unit = {

    val workDir = System.getProperty("user.dir");

    config = parseArgs(args)
    val numIndexCands = config.getInt(NUM_INDEX_CANDIDATES)
    val dbFile = config.getString(DATABASE_FILE)
    index = new RemoteIndex(config.getString(INDEX_PATH), debug = config.getBoolean(DEBUG))

    for (i <- Seq("5","10","20a","20b","20c","20d","40","50","60","80")) {
      run(s"$workDir/data/companies$i", "revenues|revenue|sales", "company", numIndexCands, dbFile)
      // run(s"$workDir/data/companies$i", "profit|earnings|profits", "company", numIndexCands, dbFile)
      run(s"$workDir/data/companies$i", "established|founded", "company", numIndexCands, dbFile)
      run(s"$workDir/data/companies$i", "employees", "company", numIndexCands, dbFile)
    }
    for (i <- Seq("Random20a","Random20b","Random20c","Random20d")) {
      run(s"$workDir/data/companies$i", "revenues|revenue|sales", "company", numIndexCands, dbFile)
      // run(s"$workDir/data/companies$i", "profit|earnings|profits", "company", numIndexCands, dbFile)
      run(s"$workDir/data/companies$i", "established|founded", "company", numIndexCands, dbFile)
      run(s"$workDir/data/companies$i", "employees", "company", numIndexCands, dbFile)
    }
    for (i <- Seq("20a","20b","20c","20d")) {
      run(s"$workDir/data/cities$i", "population|inhabitants", "city", numIndexCands, dbFile)
    }
    for (i <- Seq("Random20a","Random20b","Random20c","Random20d")) {
      run(s"$workDir/data/cities$i", "population|inhabitants", "city", numIndexCands, dbFile)
    }
    for (i <- Seq("20a","20b","20c","20d")) {
      run(s"$workDir/data/countries$i", "population", "country", numIndexCands, dbFile)
      run(s"$workDir/data/countries$i", "population growth", "country", numIndexCands, dbFile)
      run(s"$workDir/data/countries$i", "size|area", "country", numIndexCands, dbFile)
    }
  }

  def run(entitiesPath:String, attributeStr:String, conceptStr: String, numIndexCands:Int, dbFile:String):Unit = {
    val entitiesRaw = getRawEntities(entitiesPath)
    println("TARGET ENTITIES:\n  " + entitiesRaw.mkString("\n  "))

    val entities = entitiesRaw.map(analyze)
    val attribute = attributeStr.split('|')
    println("QUERIED ATTRIBUTE: "+attribute.mkString(" - "))
    val concept = conceptStr.split('|')
    var retrievalRunTime:Long = 0
    val req = new REARequest(concept, entities, attribute, numIndexCands, useEntityTables)
    val results = retrieveMatchResults(req) match {
      case Some(mr) => mr
      case None => {
        val rea = new REA(index, req)
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
    val topResults = rankedResults//.take(300)
    out(s"${results.size} results, using "+topResults.size)

    val coverableEntities = entities.indices.filter(
      ei => topResults.exists(
        r => r.coveredEntities(ei)))
    out(coverableEntities.size+" coverable entities: "+coverableEntities.map(entities(_)))


    var algos = List[SetCoverer]()
    for (k <- Seq(3,5,10)) {
      for ((th,tc) <- Seq( (0.4, 0.0), (0.0, 1.0), (0.4, 0.9) )) {
        algos ::= new GreedySetCoverer(k, coverableEntities, topResults, thCons=th, thCov=tc)
          with ResultScorer
          with ScoreWeighter
          with InverseSimByEntityDistance
          with ExponentialDiversityPicker

        // for (searchSpaceFactor <- Seq(0, 1, 3, 5, 10, 20)) {
        for (searchSpaceFactor <- Seq(10)) {
          algos ::= new MultiGreedySetCoverer(k, coverableEntities, topResults, searchSpaceFactor, thCons=th, thCov=tc)
            with ResultScorer
            with ScoreWeighter
            with InverseSimByEntityDistance
            with ExponentialDiversityPicker
            with GreedyResultSelector
          algos ::= new MultiGreedySetCoverer(k, coverableEntities, topResults, searchSpaceFactor, thCons=th, thCov=tc)
            with ResultScorer
            with ScoreWeighter
            with InverseSimByEntityDistance
            with ExponentialDiversityPicker
            with ReplacingResultSelector
            with ReplacementDecider13
          algos ::= new MultiGreedySetCoverer(k, coverableEntities, topResults, searchSpaceFactor, thCons=th, thCov=tc)
            with ResultScorer
            with ScoreWeighter
            with InverseSimByEntityDistance
            with ExponentialDiversityPicker
            with DiversifyingGreedyResultSelector


          algos ::= new GeneticSetCovererQ(k, coverableEntities, topResults, searchSpaceFactor, thCons=th, thCov=tc)
            with ResultScorer
            with ScoreWeighter
            with InverseSimByEntityDistance
            with ExponentialDiversityPicker
            with GreedyResultSelector
          algos ::= new GeneticSetCovererQ(k, coverableEntities, topResults, searchSpaceFactor, thCons=th, thCov=tc)
            with ResultScorer
            with ScoreWeighter
            with InverseSimByEntityDistance
            with ExponentialDiversityPicker
            with ReplacingResultSelector
            with ReplacementDecider13
          algos ::= new GeneticSetCovererQ(k, coverableEntities, topResults, searchSpaceFactor, thCons=th, thCov=tc)
            with ResultScorer
            with ScoreWeighter
            with InverseSimByEntityDistance
            with ExponentialDiversityPicker
            with DiversifyingGreedyResultSelector
        }
    }
      algos ::= new TopKGroupingSetCoverer(k, coverableEntities, topResults)
        with ResultScorer
        with ScoreWeighter
        with InverseSimByEntityDistance
    }
    algos ::= new GroupingSetCoverer(coverableEntities, topResults)
      with ResultScorer
      with ScoreWeighter
      with InverseSimByEntityDistance

    val store = new ResultStore(dbFile)
    for (coverer <- algos) {
      collectOutput()
      out(s"${coverer} \n=============")

      val startT = System.currentTimeMillis
      val covers = coverer.covers()
      val runTime = System.currentTimeMillis - startT
      val scores = doScoring(covers, coverer)
      printCovers(covers, coverer, entities)
      printScores(scores)
      val exId = store.storeExperiment(entitiesPath, coverer.k, entities.size,
        coverableEntities.size, covers.size, attributeStr, conceptStr,
        coverer, runTime, retrievalRunTime, scores, collectOutput(), topResults.size, covers, entities)
    }
    store.close()
  }

}
