package rea.coherence

import scala.io.Source
import rea.util.{jaccard, unitMatrix, javaSet}
import scala.math.{log10, abs, min}
import de.tudresden.matchtools.MatchTools
import de.tudresden.matchtools.SimMatrix
import rea.definitions.Dataset
import rea.definitions.REARequest
import rea.definitions.DrilledDataset
import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import rea.Config


object Coherence {

  val matchTools = MatchTools.DEFAULT

  def coherenceMatrix(results:Seq[DrilledDataset]) = {
    val matrix = unitMatrix(results.size)
    val size = results.size
    var i = 0
    var j = 0
    while (i < size) {
      j = 0
      while (j < size) {
        if (j > i) {
          val c = coherence(results(i), results(j))
          matrix(i)(j) = c
          matrix(j)(i) = c
        }
        j += 1
      }
      i +=1
    }
    // normalize(matrix)
    matrix
  }

  private def normalize(matrix:Array[Array[Double]]) = {
    var i = 0
    var j = 0
    var size = matrix.size
    val flat = matrix.flatten
    val maxVal = flat.max
    val minVal = flat.min - 0.001
    while (i < size) {
      j = 0
      while (j < size) {
        val x = matrix(i)(j)
        matrix(i)(j) = (x - minVal) / (maxVal - minVal)
        j += 1
      }
      i +=1
    }

  }

  def coherence(a:DrilledDataset, b:DrilledDataset) = {
    val aC = attributeCoherence(a, b)
	  val dC = domainCoherence(a, b)*0.8
	  val oomC = oomCoherence(a, b)
    val tagC = tagCoherence(a, b)
	  // val teC = termCoherence(a, b)*0.25
	  // val tiC = titleCoherence(a, b)*0.25
	  // min((aC+dC+oomC+teC+tiC) / 3.3, 1.0)
    min((aC+dC+oomC+tagC) / 3.8, 1.0)
  }

  def explainCoherence(a:DrilledDataset, b:DrilledDataset) = {
    val aC = attributeCoherence(a, b)
    val dC = domainCoherence(a, b)*0.8
    val oomC = oomCoherence(a, b)
    val coherence = min((aC+dC+oomC) / 2.8, 1.0)
    println(s"Coherence: $coherence, AC:$aC DC:$dC, OOMC:$oomC")
    // val teC = termCoherence(a, b)*0.25
    // val tiC = titleCoherence(a, b)*0.25
    // min((aC+dC+oomC+teC+tiC) / 3.3, 1.0)
  }

  def tagCoherence(a:DrilledDataset, b:DrilledDataset) =
    if (a.tags.size == 0 && b.tags.size == 0)
      0.0
    else
      jaccard(a.tags, b.tags)

  def attributeCoherence(a:DrilledDataset, b:DrilledDataset) =
    matchTools.byWordLevenshtein.similarity(a.normalizedMatchedAttribute, b.normalizedMatchedAttribute)

  def domainCoherence(a:DrilledDataset, b:DrilledDataset) =
    if (a.dataset.domain == b.dataset.domain)
      1.0
    else
      0.0

  def oomCoherence(a:DrilledDataset, b:DrilledDataset) = {
    val oomDiff = abs(log10(a.stats.getMean()) - log10(b.stats.getMean()))
    if (oomDiff < 1.0)
      1.0 - oomDiff
    else
      0.0
  }

  def termCoherence(a: DrilledDataset, b: DrilledDataset) =
    jaccard(a.dataset.termSet, b.dataset.termSet)

  def titleCoherence(a: DrilledDataset, b: DrilledDataset) =
    jaccard(a.dataset.titleTermSet, b.dataset.titleTermSet)

}