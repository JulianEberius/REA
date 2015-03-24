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
import java.util.Calendar

object CorrectnessTest extends REATest {

  var config:JSAPResult = null

  def main(args: Array[String]):Unit = {
    config = parseArgs(args)
    val entitiesRaw = getRawEntities(config.getString(ENTITIES_PATH))
    println("Input:\n" + entitiesRaw.mkString("\n"))

    val entities = entitiesRaw.map(analyze)
    val attribute = config.getString(ATTRIBUTE).split('|')
    val concept = config.getString(CONCEPT).split('|')
    val index = if (config.getBoolean(LOCAL_INDEX))
      new LocalIndex(config.getString(INDEX_PATH), debug = config.getBoolean(DEBUG))
    else
      new RemoteIndex(config.getString(INDEX_PATH), debug = config.getBoolean(DEBUG))
    val useEntityTables = !config.getBoolean(NO_ENTITY_TABLES)

    var retrievalRunTime:Long = 0
    val req = new REARequest(concept, entities, attribute, config.getInt(NUM_INDEX_CANDIDATES), useEntityTables)
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
    println(s"${results.size} results, using"+topResults.size )
    val attrString = config.getString(ATTRIBUTE)

    val coverableEntities = entities.indices.filter(
      ei => topResults.exists(
        r => r.coveredEntities(ei)))

    val searchSpaceFactor = config.getInt(SEARCH_SPACE_FACTOR)
    var algos = List[SetCoverer]()
    // val combinations = Seq( (0.0, 1.0), (0.4, 0.9), (0.4,0.0) )
    val combinations = Seq( (0.0,1.0))
    for ((th,tc) <- combinations) {
      // algos ::= new GreedySetCoverer(config.getInt(NUM_COVERS), coverableEntities, topResults, thCons=th, thCov=tc)
      //   with ResultScorer
      //   with ScoreWeighter
      //   with InverseSimByEntityDistance
      //   with ExponentialDiversityPicker

      // algos ::= new GreedySetCoverer(config.getInt(NUM_COVERS), coverableEntities, topResults, thCons=th, thCov=tc)
      //   with ResultScorer
      //   with ScoreWeighter
      //   with InverseSimByEntityDistance
      //   with ExponentialDiversityDynamicPicker

      // algos ::= new MultiGreedySetCoverer(config.getInt(NUM_COVERS), coverableEntities, topResults, searchSpaceFactor, thCons=th, thCov=tc)
      //   with ResultScorer
      //   with ScoreWeighter
      //   with InverseSimByEntityDistance
      //   with ExponentialDiversityPicker
      //   with GreedyResultSelector
      // algos ::= new MultiGreedySetCoverer(config.getInt(NUM_COVERS), coverableEntities, topResults, searchSpaceFactor, thCons=th, thCov=tc)
      //   with ResultScorer
      //   with ScoreWeighter
      //   with InverseSimByEntityDistance
      //   with ExponentialDiversityPicker
      //   with ReplacingResultSelector
      //   with ReplacementDecider13

     //    with ResultScorer
     //    with ScoreWeighter
     //    with InverseSimByEntityDistance
     //    with ExponentialDiversityPicker
     //    with GreedyResultSelector
     //  algos ::= new GeneticSetCovererZ(config.getInt(NUM_COVERS), coverableEntities, topResults, searchSpaceFactor, th, tc)
     //    with ResultScorer
     //    with ScoreWeighter
     //    with InverseSimByEntityDistance
     //    with ExponentialDiversityPicker
     //    with ReplacingResultSelector
     //    with ReplacementDecider13

     // algos ::= new MultiGreedySetCoverer(config.getInt(NUM_COVERS), coverableEntities, topResults, searchSpaceFactor, thCons=th, thCov=tc)
     //    with ResultScorer
     //    with ScoreWeighter
     //    with InverseSimByEntityDistance
     //    with ExponentialDiversityPicker
     //    with DiversifyingGreedyResultSelector
     // algos ::= new GeneticSetCovererZ(config.getInt(NUM_COVERS), coverableEntities, topResults, searchSpaceFactor, th, tc)
     //    with ResultScorer
     //    with ScoreWeighter
     //    with InverseSimByEntityDistance
     //    with ExponentialDiversityPicker
     //    with DiversifyingGreedyResultSelector

      algos ::= new GeneticSetCovererQ(config.getInt(NUM_COVERS), coverableEntities, topResults, searchSpaceFactor, th, tc)
        with ResultScorer
        with ScoreWeighter
        with InverseSimByEntityDistance
        with ExponentialDiversityPicker
        with DiversifyingGreedyResultSelector
     //  algos ::= new MultiGreedySetCoverer(config.getInt(NUM_COVERS), coverableEntities, topResults, searchSpaceFactor, thCons=th, thCov=tc)
     //    with ResultScorer
     //    with ScoreWeighter
     //    with InverseSimByEntityDistance
     //    with ExponentialDiversityDynamicPicker
     //    with DiversifyingGreedyResultSelector
     // algos ::= new GeneticSetCovererZ(config.getInt(NUM_COVERS), coverableEntities, topResults, searchSpaceFactor, th, tc)
     //    with ResultScorer
     //    with ScoreWeighter
     //    with InverseSimByEntityDistance
     //    with ExponentialDiversityDynamicPicker
     //    with DiversifyingGreedyResultSelector
    }

    // algos ::= new GroupingSetCoverer(coverableEntities, topResults)
    //   with ResultScorer
    //   with ScoreWeighter
    //   with InverseSimByEntityDistance
    // algos ::= new TopKGroupingSetCoverer(config.getInt(NUM_COVERS), coverableEntities, topResults)
    //   with ResultScorer
    //   with ScoreWeighter
    //   with InverseSimByEntityDistance

    val rStore = new ResultStore(config.getString(DATABASE_FILE))

    for (coverer <- algos) {
      val startT = System.currentTimeMillis
      val covers = coverer.covers()
      val runTime = System.currentTimeMillis - startT
      val scores = doScoring(covers, coverer)
      val exId = rStore.storeExperiment(config.getString(ENTITIES_PATH), config.getInt(NUM_COVERS), entities.size,
        coverableEntities.size, covers.size, config.getString(ATTRIBUTE), config.getString(CONCEPT),
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

            if (anyCorrect && !store.tagsExist(ds, attrString)) {
              println()
              val autoTags = ds.tags
              println("tags: " + autoTags.mkString(","))
              val input = readLine()
              val tags = if (input.length == 0 && autoTags.size > 0)
                  autoTags.toSet[String]
                else
                  input.split(",").toSet[String]
              store.storeTags(ds, attrString, tags)
            }
            if (!store.domainTagsExist(ds, attrString)) {
              store.addDomainTag(ds, attrString)
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
