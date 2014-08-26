package rea.util

import java.net.URL
import de.tudresden.matchtools.weights.Weighter
import rea.index.Index
import scala.collection.mutable
import rea.definitions.Dataset
import scala.collection.generic.Clearable
import scala.math.log
import java.util.regex.Pattern
import java.util.Map
import java.util.HashMap
import java.net.MalformedURLException
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisConnectionException
import rea.Config
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig

object Implicits {
  implicit class IntegerUtils(wrapped:Int) {
      def !() = (2 to wrapped).product
  }
}

object RedisConnector {
  val pool = new JedisPool(new JedisPoolConfig(), Config.redisHost);

  def getConn(db: Int): Jedis = {
    val jedis = pool.getResource
    jedis.select(db)
    return jedis
  }

  def release(jedis: Jedis) {
    pool.returnResourceObject(jedis)
  }
}

object LocalCache {

  var warned = false

  def withCache(db: Int)(cache: String, key: String, f: String => String): String = {
    val jedis = RedisConnector.getConn(db)
    try {
      val cached = jedis.get(cache + ":" + key)
      if (cached != null)
        return cached
      else {
        val result = f(key)
        jedis.set(cache + ":" + key, result)
        return result
      }
    } catch {
      case e: JedisConnectionException => {
        if (!warned) {
          System.err.println("WARNING: No connection to jedis cache at " + jedis.getClient().getHost())
          warned = true
        }
        return f(key)
      }
    } finally {
      RedisConnector.release(jedis)
    }
  }
}

object StopWatch {
  var t: Double = 0.0
  var enabled = true

  def reset = { t = System.currentTimeMillis() }
  def step(stepName: String) = {
    if (enabled) {
      val lastT = t
      t = System.currentTimeMillis()
      println(s"$stepName: ${t - lastT}")
    }
  }
}

class DFWeighter(index: Index) extends Weighter {
  val cache = mutable.Map[String, Double]()
  val DEFAULT = java.lang.Double.MAX_VALUE

  // warmUp()

  override def weight(s: String): Double = {
    var tf:Double = 0.0
    // retrieve term freq from cache or index
    if (cache.contains(s)) {
      tf = cache(s)
    } else {
      tf = index.termFrequency(s)
      cache.put(s, tf)
    }

    // calculation based on tf
    if (tf == 0.0)
      tf = DEFAULT
    tf = 1.0 / tf;
    return tf;
  }

  def warmUp() = {
    val tfCache = new Jedis(Config.redisHost)
    tfCache.select(Config.REDIS_TF_DB)

    val keys = tfCache.keys("*").iterator()
    while (keys.hasNext()) {
      val k = keys.next()
      cache.put(k, tfCache.get(k).toDouble)
    }

    println(s"TF cache warmup done: ${cache.size} keys")
  }
}

class DefaultWeighter extends Weighter {
  val cache = mutable.Map[String, Double]()
  val DEFAULT = Int.MaxValue

  override def weight(s: String): Double =
    cache.getOrElseUpdate(s, { 1.0 })
}

abstract class DatasetFilter {
  val cache: Clearable

  def filter(d: Dataset): Boolean

  def reset() = cache.clear()
}

class IdentityByContentHashFilter extends DatasetFilter {

  val cache = mutable.Set[String]()

  def filter(d: Dataset) = {
    val sb = new StringBuilder()
    d.relation.foreach {
      c =>
        sb.append(c(0))
        sb.append(c(1))
    }
    val contentSample = sb.toString()
    if (cache add contentSample) { // if new entry
      false // do not filter
    } else {
      true // existing entry, filter!
    }

  }
}

class ByTitleAndPositionFilter extends DatasetFilter {

  val cache = mutable.Set[(String,Int)]()

  def filter(d: Dataset) = {
    val signature = (d.title, d.tableNum)
    if (cache add signature) { // if new entry
      false // do not filter
    } else {
      true // existing entry, filter!
    }

  }
}

object Test {
  def main(args: Array[String]) = {
    val dfw = new DFWeighter(null)
    dfw.warmUp()
  }
}