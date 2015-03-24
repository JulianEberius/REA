package rea.server

import scala.collection.JavaConverters.mapAsJavaMapConverter
import spark._
import spark.Spark._
import rea.{REA, REA2}
import rea.index._
import rea.definitions._
import rea.analysis.analyze
import rea.index.LocalIndex
import rea.cover._
import rea.cover.SetCoverer.printCovers
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import rea.util.DFWeighter
import rea.util._
import scala.util.Random
import de.tudresden.matchtools.weights.Weighter

abstract class JsonTransformer(path: String, contentType: String)
  extends ResponseTransformerRoute(path, contentType) {
  override def render(model: AnyRef): String = model match {
    case j: JSONSerializable => j.toJson
    case j => j.toString
  }
}

object IndexServer {

  val indexDir = null
  val rnd = new Random()
  var selectivity = 1.0
  var fuzziness = 0.0

  def setup(args: Array[String]): (Index, Weighter) = {
    var port = 8765
    var indexPath:String = null

    if (args.size >= 2) {
      port = args(0).toInt
      indexPath = args(1)
      if (indexPath.trim == "")
        indexPath = null
    }
    val warmUp = (args.size == 3) && (args(2) == "-w")
    setPort(port)

    println("ARGS: "+args.mkString(" -- "))
    println("port: " + port)
    println("index: " + indexPath)
    println("warmup: " + warmUp)

    val index = if (indexPath != null)
        new LocalIndex(indexPath, debug=false, useFilteredApi=true)
      else
        new NoopIndex()
    val weighter = new DFWeighter(index, doWarmUp=warmUp)
    (index, weighter)
  }

  case class DrillbeyondResponse(
    candidates:Array[DrillbeyondSolution],
    inUnion:Array[Boolean]) extends JSONSerializable // true if the entitiy fullfills the predicate in all candidate sources
  case class DrillbeyondSolution(values:Array[java.lang.Double], selectivity:Double)

  case class SelectivityEstimates(selectivities:Array[ScoredSelectivity]) extends JSONSerializable
  case class SelectivityEstimate(selectivity:Double) extends JSONSerializable

  def unionSelect(pred:Option[Predicate], solutions: Seq[DrillbeyondSolution]): Array[Boolean] = {
    val n = solutions(0).values.length
    pred match {
      case None => (0 until n).map(_=>true).toArray
      case Some(p) =>
        (0 until n).map(i =>
          solutions
            .map(_.values(i))
            .exists(v_i => p.evaluate(v_i))) // pick all that are selected with any candidate
            .toArray
    }
  }

  def main(args: Array[String]) {
    val (index, weighter) = setup(args)

    post(new JsonTransformer("/", "application/json") {
      override def handle(request: Request, response: Response) = {
        val drb_req = JSONSerializable.fromJson(classOf[DrillbeyondRequest], request.body)
        val reqNumResults = 100 // drb_req.max_cands*10
        val req = drb_req.toREARequest(numResults=reqNumResults)
        // println(s"[${fTime}] received REA req. pred: ${req.predicates}")
        val entities = req.entities.map(analyze)

        val rea = new REA2(index, req, weighter)
        val results = rea.process()
        val rankedResults = results.toSeq.sortBy(d => (d.values.size / entities.size.toDouble) * d.score).reverse
        // filter non discriminative
        val numericValuedPredicates = req.predicates.filter(_.isInstanceOf[NumericValuedPredicate])
        val discResults =
          if (numericValuedPredicates.isEmpty)
            rankedResults
          else
            rankedResults.filter {
              case drilledDS =>
                numericValuedPredicates.exists(p => p.discriminates(drilledDS.values.map(_.value)))
            }
        val topResults = discResults
        // println(s"numCandidates: ${rankedResults.size}")

        val coverableEntities = entities.indices.filter(
          ei => topResults.exists(
            r => r.coveredEntities(ei)))
        // println(s"coverableEntities: ${coverableEntities.size}")

        // val algo = new GreedySetCoverer(drb_req.max_cands, coverableEntities, topResults, 0.0, 1.0)
        //    with ResultScorer
        //    with ScoreWeighter
        //    with InverseSimDistance
        //    with ExponentialDiversityPicker
        val algo = new GeneticSetCovererXE(drb_req.max_cands, coverableEntities, topResults, 10, 0.0,1.0)
          with ResultScorer
          with ScoreWeighter
          with InverseSimDistance
          with ExponentialDiversityPicker
          with GreedyResultSelector

        val covers = algo.covers()
        // printCovers(covers, algo, entities)
        val pred = req.predicates.find(a => a.isInstanceOf[NumericValuedPredicate]).asInstanceOf[Option[NumericValuedPredicate]]

        val candidates = covers.map(c => {
          val pickMap = c.picks.flatten.map(v => (v.forIndex, v.value)).toMap
          val vals:Array[java.lang.Double] = (
            for (i <- entities.indices) yield (
              pickMap.getOrElse(i, null).asInstanceOf[Any] match {
                case d:Double => new java.lang.Double(d)
                case _ => new java.lang.Double(-1.0)
              })
          ).toArray
          val sel = req.predicates(0).selectivity(vals)
          DrillbeyondSolution(vals, sel)
        })
        val output = DrillbeyondResponse(candidates.toArray, unionSelect(pred, candidates))
        val unionSelectivity = output.inUnion.count(_ == true) / output.inUnion.size.toDouble
        val sels:Seq[Double] = output.candidates.map(_.selectivity)
        println(s"${req.attribute.mkString(" ")},${req.predicates(0)},${drb_req.max_cands},${sels.sum / sels.size},${unionSelectivity}")
        output
      }
    })
  post(new JsonTransformer("/artificial", "application/json") {
      override def handle(request: Request, response: Response) = {
        val drb_req = JSONSerializable.fromJson(classOf[DrillbeyondRequest], request.body)
        val reareq = drb_req.toREARequest()
        val entities = reareq.entities.map(analyze)
        println(s"[${fTime}]  received artificial values req  size: " + entities.size + " current selectivity: " + selectivity + " current fuzziness :"+fuzziness)

        val pred = reareq.predicates.find(a => a.isInstanceOf[NumericValuedPredicate]).asInstanceOf[Option[NumericValuedPredicate]]
        val output = if (pred.isEmpty) {
          val candidates = (0 until reareq.numResults).map(i => {
            val vals:Array[java.lang.Double] = entities.map(e =>  new java.lang.Double(new Random(e.hashCode + 1000000*i).nextDouble))
            DrillbeyondSolution(vals, 1.0)
          })
          DrillbeyondResponse(candidates.toArray, unionSelect(pred, candidates))
        }
        else
        {
          val predVal:Double = pred.get.value
          val candidates = (0 until reareq.numResults).map(i => {

            val vals = pred.get.artificial_values(entities, selectivity, (i+1)*10000, fuzziness)
            DrillbeyondSolution(
              vals,
              reareq.predicates(0).selectivity(vals))
            })
          DrillbeyondResponse(candidates.toArray, unionSelect(pred, candidates))
        }
        val unionSelectivity = output.inUnion.count(_ == true) / output.inUnion.size.toDouble
        println(s"[${fTime}]  finished artificial values req. Selectivities: ${output.candidates.map(_.selectivity).mkString(" ")}. inUnion selectivity: ${unionSelectivity}")
        output
      }
    })
    get(new Route("/set_selectivity/:sel") {
      override def handle(request: Request, response: Response) = {
        val reqSel = request.params(":sel")
        selectivity = reqSel.toDouble
        "Ok"
      }
    })
    get(new Route("/set_fuzziness/:fuz") {
      override def handle(request: Request, response: Response) = {
        val reqFuzz = request.params(":fuz")
        fuzziness = reqFuzz.toDouble
        "Ok"
      }
    })
    post(new JsonTransformer("/rea", "application/json") {
      override def handle(request: Request, response: Response) = {
        val req = JSONSerializable.fromJson(classOf[REARequest], request.body)
        val entities = req.entities.map(analyze)

        val rea = new REA(index, req, weighter)
        val results = rea.process()
        val rankedResults = results.toSeq.sortBy(d => (d.values.size / entities.size.toDouble) * d.score).reverse
        val topResults = rankedResults//.take(300)
        println(s"numCandidates: ${rankedResults.size}")

        val coverableEntities = entities.indices.filter(
          ei => topResults.exists(
            r => r.coveredEntities(ei)))
        println(s"coverableEntities: ${coverableEntities.size}")

        val algo = new GreedySetCoverer(req.numResults, coverableEntities, topResults, 0.0, 1.0)
           with ResultScorer
           with ScoreWeighter
           with InverseSimDistance
           with ExponentialDiversityPicker

        val covers = algo.covers()
        // printCovers(covers, algo, entities)

        val pred = req.predicates.find(a => a.isInstanceOf[NumericValuedPredicate]).asInstanceOf[Option[NumericValuedPredicate]]
        val candidates = covers.map(c => {
          val pickMap = c.picks.flatten.map(v => (v.forIndex, v.value)).toMap
          val vals:Array[java.lang.Double] = (
            for (i <- entities.indices) yield (
              pickMap.getOrElse(i, null).asInstanceOf[Any] match {
                case d:Double => new java.lang.Double(d)
                case _ => new java.lang.Double(-1.0)
              })
          ).toArray
          val sel = req.predicates(0).selectivity(vals)
          DrillbeyondSolution(vals, sel)
        })
        val output = DrillbeyondResponse(candidates.toArray, unionSelect(pred, candidates))
        // println("RESULT: "+output.toJson)
        output
      }
    })

    post(new JsonTransformer("/augmentRelation", "application/json") {
      override def handle(request: Request, response: Response) = {
        val req = JSONSerializable.fromJson(classOf[REARequest], request.body)
        val result = index.search(req.attribute, req.entities.toArray, req.concept, req.numResults, filteredApi=false)
        result.getOrElse("{}")
      }
    })
     post(new JsonTransformer("/augmentRelationFiltered", "application/json") {
      override def handle(request: Request, response: Response) = {
        val req = JSONSerializable.fromJson(classOf[REARequest], request.body)
        val result = index.search(req.attribute, req.entities.toArray, req.concept, req.numResults, filteredApi=true)
        result.getOrElse("{}")
      }
    })
    post(new JsonTransformer("/augmentRelationFiltered2", "application/json") {
      override def handle(request: Request, response: Response) = {
        val req = JSONSerializable.fromJson(classOf[REARequest], request.body)
        println("REQUEST /augmentRelationFiltered2: "+req)
        val result = index.search2(req.attribute, req.entities.toArray, req.concept, req.numResults)
        result.getOrElse("{}")
      }
    })
    post(new JsonTransformer("/augmentEntity", "application/json") {
      override def handle(request: Request, response: Response) = {
        val req = JSONSerializable.fromJson(classOf[REARequest], request.body)
        val result = index.searchSingle(req.attribute, req.entities(0), req.concept, req.numResults, filteredApi=false)
        result.getOrElse("{}")
      }
    })
    post(new JsonTransformer("/augmentEntityFiltered", "application/json") {
      override def handle(request: Request, response: Response) = {
        val req = JSONSerializable.fromJson(classOf[REARequest], request.body)
        val result = index.searchSingle(req.attribute, req.entities(0), req.concept, req.numResults, filteredApi=true)
        result.getOrElse("{}")
      }
    })
    post(new JsonTransformer("/augmentEntityFiltered2", "application/json") {
      override def handle(request: Request, response: Response) = {
        val req = JSONSerializable.fromJson(classOf[REARequest], request.body)
        println("REQUEST /augmentEntityFiltered2: "+req)
        val result = index.searchSingle2(req.attribute, req.entities(0), req.concept, req.numResults)
        result.getOrElse("{}")
      }
    })
    post(new JsonTransformer("/continueSearch", "application/json") {
      override def handle(request: Request, response: Response) = {
        val req = JSONSerializable.fromJson(classOf[REAContinueRequest], request.body)
        val result = index.continueSearch(req.handle, req.numResults)
        result.getOrElse("{}")
      }
    })
    post(new JsonTransformer("/termFrequency", "application/json") {
      override def handle(request: Request, response: Response) = {
        val req = JSONSerializable.fromJson(classOf[REATermFrequencyRequest], request.body)
        val result = index.termFrequency(req.term)
        TermFrequencyResult(result)
      }
    })
    post(new JsonTransformer("/datasetByURL", "application/json") {
      override def handle(request: Request, response: Response) = {
        val req = JSONSerializable.fromJson(classOf[READatasetRequest], request.body)
        index.findDatasetByURL(req.url).getOrElse("{}")
      }
    })
    post(new JsonTransformer("/randomDataset", "application/json") {
      override def handle(request: Request, response: Response) = {
        val req = JSONSerializable.fromJson(classOf[READatasetRequest], request.body)
        index.randomDataset().getOrElse("{}")
      }
    })
    post(new JsonTransformer("/datasetByIndex", "application/json") {
      override def handle(request: Request, response: Response) = {
        val req = JSONSerializable.fromJson(classOf[READatasetRequest], request.body)
        index.findDatasetByIndex(req.url.toInt).getOrElse("{}")
      }
    })
    post(new JsonTransformer("/entitySearch", "application/json") {
      override def handle(request: Request, response: Response) = {
        val req = JSONSerializable.fromJson(classOf[REAEntitySearchRequest], request.body)
        val result = index.entitySearch(req.concept, req.attributes, req.seed, req.numResults)
        result.getOrElse("{}")
      }
    })
    post(new JsonTransformer("/selectivityEstimates", "application/json") {
      override def handle(request: Request, response: Response) = {
        val req = JSONSerializable.fromJson(classOf[REARequest], request.body)
        val result = Selectivity.estimates(req, index) match {
          case None => SelectivityEstimates(Array())
          case Some(sels) => SelectivityEstimates(sels.toArray)
        }
        result
      }
    })
    post(new JsonTransformer("/estimatedSelectivity", "application/json") {
      override def handle(request: Request, response: Response) = {
        println(s"[${fTime}]  received req")
        val req = JSONSerializable.fromJson(classOf[REARequest], request.body)
        println(s"[${fTime}]  parsed req")
        println("REQUEST /estimatedSelectivity: "+req)
        val result = Selectivity.estimate(req, index) match {
          case None => None
          case Some(sel) => Some(SelectivityEstimate(sel))
        }
        result.getOrElse("{}")
      }
    })
    post(new JsonTransformer("/drb_estimatedSelectivity", "application/json") {
      override def handle(request: Request, response: Response) = {
        val drb_req = JSONSerializable.fromJson(classOf[DrillbeyondRequest], request.body)
        // val reqNumResults = 100
        val req = drb_req.toREARequest()//numResults=reqNumResults)//, entityTables=false)
        val result = Selectivity.estimate(req, index) match {
          case None => None
          case Some(sel) => Some(SelectivityEstimate(sel))
        }
        result.getOrElse("{}")
      }
    })
  }
}
