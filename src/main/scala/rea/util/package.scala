package rea

import scala.math.min
import java.util.regex.Pattern
import java.net.URL
import java.net.URI
import java.net.MalformedURLException
import scala.collection.mutable
import rea.analysis.analyze
import de.tudresden.matchtools.similarities.SetSimilarities
import scala.annotation
import java.text.SimpleDateFormat
import java.util.Date

package object util {

  val df = new SimpleDateFormat("HH:mm:ss:SSS")

  def fTime():String = fTime(System.currentTimeMillis)

  def fTime(s:Long):String = {
    df.format(new Date(s))
  }

  def naturalTime(s:Int) = {
    if (s / 60 > 0)
      if (s / 3600 > 0)
        f"${s / 3600}:${((s % 3600) / 60)}%02d h"
      else
        s"${s / 60} min"
    else
      s"$s sec"
  }

  class ProgressReporter(max:Long, interval:Int = 1000000) {
    var j = 0
    var t = System.currentTimeMillis

    def progress() = {
      j += 1
      if (j % interval == 0){
        val perSecond = (j / ((System.currentTimeMillis-t)/1000.0))
        val remaining = max-j
        val eta = (remaining / perSecond).toInt
        println(f"finished: $j of $max combinations/s: $perSecond%.2f eta: ${naturalTime(eta)}")
      }
    }
  }

  @annotation.tailrec
  def factorial(n: Int, accumulator: Long = 1): Long = {
      if(n == 0) accumulator else factorial(n - 1, (accumulator * n))
  }

  def Nn(N:Int, n:Int): Long = {
    var i = N
    var acc:Long = 1
    while (i > N-n) {
      acc *= i
      i -= 1
    }
    acc
  }

  def Nc(N:Int, n:Int): Long = {
    var i = N
    var acc:Long = 1
    while (i > N-n) {
      acc *= i
      i -= 1
    }

    var j = n
    var acc2:Long = 1
    while (j > 1) {
      acc2 *= j
      j -= 1
    }
    (acc.toDouble / acc2.toDouble).toLong
  }

  def unitMatrix(size:Int):Array[Array[Double]] = {
    val matrix = Array.ofDim[Double](size, size)
    for (i <- 0 until size)
      matrix(i)(i) = 1.0
    matrix
  }

  def argMax(xs:Array[Double]) = {
    var i = 0
    var max = 0.0
    var result = -1
    while (i < xs.size) {
      if (xs(i) > max) {
        max = xs(i)
        result = i
      }
      i += 1
    }
    result
  }

  def argMin(xs:Array[Double]) = {
    var i = 0
    var min = Double.MaxValue
    var result = -1
    while (i < xs.size) {
      if (xs(i) < min) {
        min = xs(i)
        result = i
      }
      i += 1
    }
    result
  }

  def argMax[T](xs:Iterable[T], f: (T) => Double):Option[Int] = {
    var pos = 0
    var max = 0.0
    var result = -1

    for (t <- xs) {
      val v = f(t)
      if (v > max) {
        result = pos
        max = v
      }
      pos += 1
    }
    if (result == -1)
      None
    else
      Some(result)
  }

  def argMin[T](xs:Iterable[T], f: (T) => Double):Option[Int] = {
    var pos = 0
    var min = Double.MaxValue
    var result = -1

    for (t <- xs) {
      val v = f(t)
      if (v < min) {
        result = pos
        min = v
      }
      pos += 1
    }
    if (result == -1)
      None
    else
      Some(result)
  }

  def analyzedJavaSet(xs: Iterable[String]*): java.util.Set[String] = {
    val result = new java.util.HashSet[String]()
    for (x <- xs) {
      val iter = x.iterator
      while (iter.hasNext) {
        val s = analyze(iter.next)
        if (!s.isEmpty)
          result.add(s)
      }
    }
    result
  }

  def javaSet[X](x: Iterable[X]): java.util.Set[X] = {
    val result = new java.util.HashSet[X]()
    val iter = x.iterator
    while (iter.hasNext) {
      result.add(iter.next)
    }
    result
  }

  val fastJaccard = new SetSimilarities.Jaccard[String]()
  val googleJaccard = new SetSimilarities.GoogleJaccard[String]()

  def bitSetJaccard(a: java.util.BitSet, b: java.util.BitSet) = {
    val clone_a = a.clone().asInstanceOf[java.util.BitSet]
    val clone_b = a.clone().asInstanceOf[java.util.BitSet]

    clone_a.and(b)
    clone_b.or(a)

    if (clone_b.cardinality() == 0)
      0.0
    else
      clone_a.cardinality().toDouble / clone_b.cardinality().toDouble
  }

  def jaccard(a: java.util.Set[String], b: java.util.Set[String]) =
    googleJaccard.similarity(a, b)

  def minSetCovererage(a: Set[String], b: Set[String]) = {
    val mSize = min(a.size, b.size).toDouble
    if (mSize == 0.0)
      0.0
    else
      a.intersect(b).size / mSize
  }

  val urlSplitPattern = Pattern.compile("[/_-]|%20")

  def splitURL(url: String) = urlSplitPattern.split(
    try {
      val u = new URI(url)
      val s = u.getPath()
      if (s.endsWith(".htm") || s.endsWith(".html"))
        s.substring(0, s.lastIndexOf("."))
      else
        s
    } catch {
      case e: MalformedURLException => url
    }).toSet[String]

  def main(args: Array[String]) = {
    val stest = "HSBA.L--HSBC_Holdings_PLC"
    println(splitURL(stest))
  }
}