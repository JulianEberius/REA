package rea.server

import scala.collection.JavaConverters.mapAsJavaMapConverter
import spark._
import spark.Spark._
import rea.index._
import rea.definitions._
import rea.index.LocalIndex

abstract class JsonTransformer(path: String, contentType: String)
  extends ResponseTransformerRoute(path, contentType) {
  override def render(model: AnyRef): String = model match {
    case j: JSONSerializable => j.toJson
    case j => j.toString
  }
}

object IndexServer {

  val indexDir = null

  def setup(args: Array[String]): Index = {
    var port = 9876
    var indexPath = ""

    if (args.size == 2) {
      port = args(0).toInt
      indexPath = args(1)
    }
    setPort(port)

    println("port: " + port)
    println("index: " + indexPath)
    return new LocalIndex(indexPath, debug=true)
  }

  def main(args: Array[String]) {
    val index = setup(args)

    post(new JsonTransformer("/", "application/json") {
      override def handle(request: Request, response: Response) = {
        val req = JSONSerializable.fromJson(classOf[REARequest], request.body)
        val result = index.search(req.attribute, req.entities.toArray, req.concept, req.numResults)
        result.getOrElse("{}")
      }
    })
    post(new JsonTransformer("/augmentRelation", "application/json") {
      override def handle(request: Request, response: Response) = {
        val req = JSONSerializable.fromJson(classOf[REARequest], request.body)
        val result = index.search(req.attribute, req.entities.toArray, req.concept, req.numResults)
        result.getOrElse("{}")
      }
    })
    post(new JsonTransformer("/augmentEntity", "application/json") {
      override def handle(request: Request, response: Response) = {
        val req = JSONSerializable.fromJson(classOf[REARequest], request.body)
        val result = index.searchSingle(req.attribute, req.entities(0), req.concept, req.numResults)
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
  }
}
