package rea.index

import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.TermQuery
import org.apache.lucene.search.TopDocs
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.index.{ IndexReader, DirectoryReader, Term }
import org.apache.lucene.util.Version
import org.apache.lucene.store.{ NIOFSDirectory, MMapDirectory }
import java.io.{ StringReader, File }
import java.net.URL
import org.apache.http.client.fluent._
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.queryparser.classic.QueryParserBase
import org.apache.lucene.document.Document
import org.apache.lucene.search.Query
import org.apache.lucene.search.ScoreDoc
import org.apache.lucene.analysis.standard.{ ClassicAnalyzer, StandardAnalyzer }
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import scala.collection.mutable
import org.apache.http.entity.ContentType
import com.google.gson.Gson
import org.apache.http.entity.StringEntity
import rea.definitions._
import rea.analysis._
import rea.util.LocalCache.withCache
import rea.util.RedisConnector
import java.net.InetSocketAddress
import java.util.logging.Logger
import java.util.logging.Level
import rea.Config
import rea.util.{IdentityByContentHashFilter, ByTitleAndPositionFilter}
import rea.util.DatasetFilter
import scala.reflect.ClassTag
import scala.util.Random
import rea.analysis.REAAnalyzer

// class SearchResult
case class SearchResult(datasets: Array[Dataset], handle: Int) extends JSONSerializable
case class TermFrequencyResult(frequency: Double) extends JSONSerializable

trait Index {
  def search(attribute: Array[String], relation: Array[String], concept: Array[String], numResults: Int = 30): Option[SearchResult]
  def searchSingle(attribute: Array[String], entity: String, concept: Array[String], numResults: Int = 5): Option[SearchResult]
  def continueSearch(handle: Int, numResults: Int = 30): Option[SearchResult]
  def keywordGroups(kw: String): List[List[String]]
  def termFrequency(termString: String): Double
  def findDatasetByURL(url: String): Option[SearchResult]
  def randomDataset(): Option[SearchResult]
  def findDatasetByIndex(id: Int): Option[SearchResult]
}

trait HandleStore[E] {
  private var currentHandle = 0
  private val handleMap = mutable.Map[Int, E]()
  private def nextHandle = {
    val handle = currentHandle
    currentHandle += 1
    handle
  }

  def storeWithHandle(e: E): Int = {
    val handle = nextHandle
    handleMap(handle) = e
    handle
  }

  def getByHandle(h: Int): E = handleMap(h)
}

trait KeywordQueryProcessor {
  def keywordGroups(kw: String): List[List[String]] = kw match {
    case kw if kw.contains("_") => List(
      List(kw.replace("_", " ")),
      kw.split("_").toList)
    case kw if kw.contains(" ") => List(
      List(kw),
      kw.split(" ").toList)
    case kw => List(List(kw))
  }
}

class LocalIndex(indexDir: String,val debug: Boolean = false) extends Index with HandleStore[(Query, ScoreDoc, DatasetFilter)] with KeywordQueryProcessor {

  val ENTITIES_FIELD = "entities"
  val ATTRIBUTES_FIELD = "attributes"
  val TITLE_FIELD = "title"
  val KEYS_FIELD = "keys"
  val TERMS_FIELD = "terms"
  val URL_FIELD = "url"

  var searcher: IndexSearcher = new IndexSearcher(DirectoryReader.open(new NIOFSDirectory(new File(indexDir))))
  var reader: IndexReader = searcher.getIndexReader
  var totalTermFrequency: Int = 0
  val qpa = new QueryParser(Version.LUCENE_45, ATTRIBUTES_FIELD, new REAAnalyzer())
  val qpt = new QueryParser(Version.LUCENE_45, TITLE_FIELD, new REAAnalyzer())
  val qpk = new QueryParser(Version.LUCENE_45, KEYS_FIELD, new REAAnalyzer())
  val qpterms = new QueryParser(Version.LUCENE_45, TERMS_FIELD, new REAAnalyzer())
  val qpe = new QueryParser(Version.LUCENE_45, ENTITIES_FIELD, new REAAnalyzer())
  val qpu = new QueryParser(Version.LUCENE_45, URL_FIELD, new REAAnalyzer())

  val rnd = new Random(0)

  def createQuery(attributes: Array[String], relation: Array[String], concept: Array[String]) = {
    val q = new BooleanQuery()

    // attribute must be present (better as phrase)
    val bqa = new BooleanQuery()
    for (a <- attributes) {
      val qt = qpt.parse(a)
      qt.setBoost(0.7f)
      q.add(qt, BooleanClause.Occur.SHOULD)

      // val qa = qpa.parse(a)
      // qa.setBoost(0.9f)
      val qaph = qpa.parse("\"" + a + "\"")
      // qaph.setBoost(1.7f)
      // bqa.add(qa, BooleanClause.Occur.SHOULD)
      bqa.add(qaph, BooleanClause.Occur.SHOULD)
    }
    q.add(bqa, BooleanClause.Occur.MUST)

    // concept should be present in attributes
    for (c <- concept) {
      val qc = qpa.parse(c)
      qc.setBoost(0.7f)
      q.add(qc, BooleanClause.Occur.SHOULD)
    }

    // the more entities are present, the better
    val entitySet = relation.toSet[String].filter(_ != null).take(200)
    for (e <- entitySet) {
      val qe = qpe.parse(QueryParserBase.escape(e))
      qe.setBoost(0.9f)
      val qeph = qpe.parse("\""+QueryParserBase.escape(e)+"\"")
      qeph.setBoost(1.7f)
      q.add(qe, BooleanClause.Occur.SHOULD)
      q.add(qeph, BooleanClause.Occur.SHOULD)
    }

    q
  }

  def termFrequency(termString: String) = {
    val jedis = RedisConnector.getConn(Config.REDIS_TF_DB)
    val tf = jedis.get(termString)
    val result = if (tf == null) {
      val result = reader.totalTermFreq(new Term(ENTITIES_FIELD, termString)).toDouble
      jedis.set(termString, result.toString)
      result
    } else {
      tf.toDouble
    }
    RedisConnector.release(jedis)
    result
  }

  def toDatasets(td: TopDocs) = td.scoreDocs.map(h => {
    val ds = JSONSerializable.fromJson(classOf[Dataset], searcher.doc(h.doc).get("full_result"))
    ds.indexId = h.doc
    ds
  })

  def search(attribute: Array[String], relation: Array[String], concept: Array[String], numResults: Int = 30) = {
    val t = System.currentTimeMillis
    val query = createQuery(attribute, relation, concept)
    if (debug)
      println("QUERY: " + query.toString())
    val result = searcher.search(query, numResults)
    if (debug)
      println("time: " + (System.currentTimeMillis - t))
    if (result.scoreDocs.length > 0)
      toSearchResult(query, result)
    else
      None
  }

  def toSearchResult(query: Query, result: TopDocs) = {
    val datasetFilter = new ByTitleAndPositionFilter()
    val handle = storeWithHandle((query, result.scoreDocs.last, datasetFilter))
    assert(result.scoreDocs.length > 0)
    val ds = toDatasets(result).filterNot(datasetFilter.filter(_))
    Some(SearchResult(ds, handle))
  }

  def searchSingle(attribute: Array[String], entity: String, concept: Array[String], numResults: Int = 5): Option[SearchResult] = {
    val entityQueryString = entity.split(" ").mkString(" AND ")

    val title_query = qpt.parse(QueryParserBase.escape(entityQueryString))
    val entity_query = qpa.parse(QueryParserBase.escape(entity))
    val qa = new BooleanQuery()
    val qk = new BooleanQuery()
    for (a <- attribute) {
      val sqpha = qpa.parse("\"" + a + "\"")
      val sqphk = qpk.parse("\"" + a + "\"")
      qa.add(sqpha, BooleanClause.Occur.SHOULD)
      qk.add(sqphk, BooleanClause.Occur.SHOULD)
    }

    val kw_query = new BooleanQuery()
    kw_query.add(qa, BooleanClause.Occur.SHOULD)
    kw_query.add(qk, BooleanClause.Occur.SHOULD)

    val concept_query = new BooleanQuery()
    for (c <- concept) {
      val cq = qpt.parse(c)
      concept_query.add(cq, BooleanClause.Occur.SHOULD)
    }

    val query = new BooleanQuery()
    query.add(kw_query, BooleanClause.Occur.MUST)
    query.add(title_query, BooleanClause.Occur.MUST)
    query.add(entity_query, BooleanClause.Occur.SHOULD)
    query.add(concept_query, BooleanClause.Occur.SHOULD)

    if (debug)
      println("Entity-QUERY: " + query.toString())
    val result = searcher.search(query, numResults)
    if (result.scoreDocs.length > 0)
      toSearchResult(query, result)
    else {
      val cleanedEntity = clean(entity)
      if (!cleanedEntity.equals(entity))
        searchSingle(attribute, cleanedEntity, concept, numResults)
      else
        None
    }
  }

  def continueSearch(handle: Int, numResults: Int = 30) = {
    val (query, lastDoc, filter) = getByHandle(handle)
    val result = searcher.searchAfter(lastDoc, query, numResults)
    if (result.scoreDocs.length > 0)
      toSearchResult(query, result)
    else
      None
  }

  def findDatasetByURL(url: String) = {
    val result = searcher.search(qpu.parse(QueryParserBase.escape(url)), 10)
    if (result.scoreDocs.length > 0)
      Some(SearchResult(toDatasets(result), 0))
    else
      None
  }

  def randomDataset() = {
    val numDocs = searcher.getIndexReader.numDocs
    val id = rnd.nextInt(numDocs)
    val doc = searcher.doc(id)
    val ds = JSONSerializable.fromJson(classOf[Dataset], doc.get("full_result"))
    ds.indexId = id
    Some(SearchResult(Array(ds), 0))
  }

  def findDatasetByIndex(id: Int) = {
    val doc = searcher.doc(id)
    val ds = JSONSerializable.fromJson(classOf[Dataset], doc.get("full_result"))
    ds.indexId = id
    Some(SearchResult(Array(ds), 0))
  }


  def entitySearch(concept: String, attributes: Array[String], seed: Array[String] = Array(), numResults: Int = 30) = {
    val query = new BooleanQuery()

    val att_query = new BooleanQuery()
    for (akw <- attributes)
      att_query.add(new TermQuery(new Term(ATTRIBUTES_FIELD, akw)), BooleanClause.Occur.SHOULD)
    if (concept.size > 0) {
      val concept_query = new BooleanQuery()
      for (ckw <- concept.split(' ')) {
        val one_concept_query = new BooleanQuery()
        for (ackw <- ckw.split('|')) {
          one_concept_query.add(new TermQuery(new Term(TITLE_FIELD, ackw)), BooleanClause.Occur.SHOULD)
          // one_concept_query.add(new TermQuery(new Term(TERMS_FIELD, ckw)), BooleanClause.Occur.SHOULD)
        }
        concept_query.add(one_concept_query, BooleanClause.Occur.MUST)
      }
      query.add(concept_query, BooleanClause.Occur.MUST)
    }

    query.add(att_query, BooleanClause.Occur.MUST)

    if (seed.length > 0) {
      val seed_query = new BooleanQuery()
      for (skw <- seed) {
        // for (sskw <- skw.split(" "))
        seed_query.add(qpe.parse(QueryParserBase.escape(skw)), BooleanClause.Occur.SHOULD)
      }
      query.add(seed_query, BooleanClause.Occur.MUST)
    }

    println("QUERY: " + query)
    val result = searcher.search(query, numResults)
    if (result.scoreDocs.length > 0)
      toSearchResult(query, result)
    else
      None
  }
}

class RemoteIndex(_url: String, val debug: Boolean = false) extends Index() with KeywordQueryProcessor {

  val serverUrl = if (_url.endsWith("/")) _url.dropRight(1) else _url

  private def remoteCall[E: ClassTag](url: String, req: JSONSerializable, t: Class[E]): Option[E] =
    withCache(Config.REDIS_REMOTE_INDEX)(url, req.toJson,
      r => Request.Post(url)
        .bodyString(r, ContentType.APPLICATION_JSON)
        .execute().returnContent().asString()) match {
        case "{}" => None
        case "null" => None
        case r: String => Some(JSONSerializable.fromJson(t, r))
      }

  def search(attribute: Array[String], relation: Array[String], concept: Array[String], numResults: Int = 30) = {
    val r = new REARequest(concept, relation, attribute, numResults)
    remoteCall(serverUrl + "/augmentRelation", r, classOf[SearchResult])
  }

  def searchSingle(attribute: Array[String], entity: String, concept: Array[String], numResults: Int = 5) = {
    val r = new REARequest(concept, Array(entity), attribute, numResults)
    remoteCall(serverUrl + "/augmentEntity", r, classOf[SearchResult])
  }

  def continueSearch(handle: Int, numResults: Int = 30) = {
    val r = new REAContinueRequest(handle, numResults)
    remoteCall(serverUrl + "/continueSearch", r, classOf[SearchResult])
  }

  def termFrequency(termString: String): Double = {
    val jedis = RedisConnector.getConn(Config.REDIS_TF_DB)
    val tf = jedis.get(termString)
    val result = if (tf == null) {
      val r = new REATermFrequencyRequest(termString)
      val resp = Request.Post(serverUrl + "/termFrequency")
        .bodyString(r.toJson, ContentType.APPLICATION_JSON)
        .execute().returnContent().asString()
      val result = JSONSerializable.fromJson(classOf[TermFrequencyResult], resp)
      jedis.set(termString, result.frequency.toString)
      result.frequency
    } else {
      tf.toDouble
    }
    RedisConnector.release(jedis)
    result
  }

  def findDatasetByURL(datasetURL: String) = {
    val r = new READatasetRequest(datasetURL)
    remoteCall(serverUrl + "/datasetByURL", r, classOf[SearchResult])
  }
  def randomDataset() = {
    val r = new READatasetRequest("")
    remoteCall(serverUrl + "/randomDataset", r, classOf[SearchResult])
  }
  def findDatasetByIndex(id: Int) = {
    val r = new READatasetRequest(id.toString)
    remoteCall(serverUrl + "/datasetByIndex", r, classOf[SearchResult])
  }
}
