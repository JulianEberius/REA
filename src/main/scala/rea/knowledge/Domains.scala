package rea.knowledge

import java.net.URL
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.TimeZone
import org.apache.commons.io.IOUtils
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import sun.misc.BASE64Encoder
import rea.Config
import rea.definitions.Dataset
import rea.analysis._
import rea.util.javaSet
import rea.util.RedisConnector
import java.net.URI

case class DomainInfo(
  val keywords: Seq[String],
  val categories: Seq[Seq[String]],
  val rank: Int,
  val inCount: Int) {
  val termSet = javaSet((keywords ++ categories.flatten).map(analyze))
}

object Domains {

  protected val ACTION_NAME = "UrlInfo"
  protected val SERVICE_HOST = "awis.amazonaws.com"
  protected val AWS_BASE_URL = "http://" + SERVICE_HOST + "/?"
  protected val HASH_ALGORITHM = "HmacSHA256"
  protected val DATEFORMAT_AWS = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
  protected val format = new SimpleDateFormat(DATEFORMAT_AWS)
  format.setTimeZone(TimeZone.getTimeZone("GMT"))

  protected val localCache = Map[String, DomainInfo]()

  protected val defaultResponseGroups = "Rank,LinksInCount,Categories,Keywords"
  protected val secretAccessKey:String = null
  protected val accessKeyId:String = null

  if (secretAccessKey == null || accessKeyId == null)
    throw new RuntimeException("secreAccessKey and accessKeyId need to be set in Domains.scala before compiling")

  protected def getTimestampFromLocalTime(date: Date): String =
    format.format(date)

  protected def generateSignature(data: String): String =
    {
      val signingKey = new SecretKeySpec(secretAccessKey.getBytes, HASH_ALGORITHM)
      val mac = Mac.getInstance(HASH_ALGORITHM)
      mac.init(signingKey)
      val rawHmac = mac.doFinal(data.getBytes)
      new BASE64Encoder().encode(rawHmac)
    }

  protected def buildQuery(domain: String): String =
    Map("Action" -> ACTION_NAME,
      "ResponseGroup" -> defaultResponseGroups,
      "AWSAccessKeyId" -> accessKeyId,
      "Timestamp" -> getTimestampFromLocalTime(Calendar.getInstance.getTime),
      "Url" -> domain,
      "SignatureVersion" -> "2",
      "SignatureMethod" -> HASH_ALGORITHM).toList.sorted.collect {
        case (k, v) => k + "=" + URLEncoder.encode(v, "UTF-8")
      }.mkString("&")

  protected def makeRequest(requestUrl: String): String = {
    val url = new URL(requestUrl)
    val conn = url.openConnection()
    val in = conn.getInputStream
    val response = IOUtils.toString(in)
    in.close()
    response
  }

  protected def unmarshal(response: xml.Elem): DomainInfo = {
    val alexa = response \ "Response" \ "UrlInfoResult" \ "Alexa"
    val contentData = alexa \ "ContentData"
    val related = alexa \ "Related"
    val trafficData = alexa \ "TrafficData"

    val inCountStr = (contentData \ "LinksInCount").text
    val inCount =
      if (inCountStr == "") {
        println("WHAWT 2")
        0
      }
      else
        inCountStr.toInt
    val keywords = (contentData \ "Keywords" \ "Keyword").map(kw => kw.text).toSeq
    val categories = (related \ "Categories" \ "CategoryData").map(c => (c \ "AbsolutePath").text.split("/").toSeq)

    val rankStr = (trafficData \ "Rank").text
    val rank =
      if (rankStr == "") {
        println("WHAWT")
        Integer.MAX_VALUE
      }
      else
        rankStr.toInt

    DomainInfo(keywords, categories, rank, inCount)
  }

  def query(domain: String, responseGroups: String = defaultResponseGroups): DomainInfo = {
    val jedis = RedisConnector.getConn(Config.REDIS_DOMAIN_INFO_DB)
    val cacheKey = s"$domain||$responseGroups"
    val cachedDomainInfo = localCache.get(cacheKey)
    if (cachedDomainInfo.isDefined)
      return cachedDomainInfo.get
    val cachedResponse = jedis.get(cacheKey)

    val xmlResponse = if (cachedResponse != null) {
      cachedResponse
    } else {
      val query = buildQuery(domain)
      val toSign = "GET\n" + SERVICE_HOST + "\n/\n" + query
      val signature = generateSignature(toSign)
      val uri = AWS_BASE_URL + query + "&Signature=" + URLEncoder.encode(signature, "UTF-8")
      val xmlResponse = makeRequest(uri)
      jedis.set(cacheKey, xmlResponse)
      xmlResponse
    }
    RedisConnector.release(jedis)
    return unmarshal(xml.XML.loadString(xmlResponse))
  }

  def scoreOnly(ds:Dataset):Double = {
    if (secretAccessKey == null || accessKeyId  == null)
      return 1.0

    val jedis = RedisConnector.getConn(Config.REDIS_DOMAINSCORE_DB)

    val score = jedis.get(ds.domain) match {
      case null => {
        val score = ds.domainInfo.rank.toDouble
        jedis.set(ds.domain, score.toString)
        score
      }
      case x => x.toDouble
    }
    RedisConnector.release(jedis)
     // normalization is done later
    return score
  }


  def main(args: Array[String]) {
    val x = new URI("https://www.peter.de/onsd/Fsa?sf=we2")
    val y = x.getHost
    val z = y.lastIndexOf('.')
    val result = y.lastIndexOf('.', z-1) match {
      case -1 => y
      case k => y.substring(k+1)
    }
    println(result)
//    println(query("wikipedia.org/").rank)
//    println(query("mongabay.com/").rank)
//    println(query("wikirating.org/").rank)
  }
}
