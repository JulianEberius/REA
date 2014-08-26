package rea.cover

import rea.definitions._
import rea.coherence._

abstract class GroupingSetCoverer(_entities: Seq[Int], _candidates: Seq[DrilledDataset]) extends SetCoverer(1, _entities, _candidates) {

  def name = "GroupingSetCoverer"
  override def toString = s"$name"

  override def covers(): Seq[Cover] = {
    val ddsByDs = _candidates.map(dds => (dds.dataset, dds)).toMap
    val grouper = new FuzzyGrouping(entities.toSet, _candidates)
    val groupsByEntity = grouper.cluster()
    val maxGroups = groupsByEntity.mapValues(_.maxBy(
        group => group.map(
            v => ddsByDs(v.dataset).inherentScore).sum))
    val bestVals = maxGroups.values.map(
        group => group.maxBy(v => ddsByDs(v.dataset).inherentScore))
    val bestValsByDs = bestVals.groupBy(_.dataset).map { case (k,v) => (ddsByDs(k),v.toSeq) }.toMap
    Seq(new Cover(bestValsByDs.keys.toSeq, bestValsByDs.keys.map(_candidates.indexOf(_)).toSeq, bestValsByDs.values.toSeq))
  }
}

abstract class TopKGroupingPartialSetCoverer(k:Int, _entities: Seq[Int], _candidates: Seq[DrilledDataset]) extends SetCoverer(k, _entities, _candidates) {

  def name = "TopKGroupingPartialSetCoverer"
  override def toString = s"$name"

  override def covers(): Seq[Cover] = {
    val ddsByDs = _candidates.map(dds => (dds.dataset, dds)).toMap
    val groupsByEntity:Map[Int,Seq[DrilledValue]] = _candidates.flatMap(_.values).groupBy(_.forIndex).toMap
    val sortedGroups = groupsByEntity.mapValues(_.sortBy(v => ddsByDs(v.dataset).inherentScore).reverse)
    (0 until k).map(i => {
      val bestVals = sortedGroups.values.flatMap(group => group.lift(i))
      val bestValsByDs = bestVals.groupBy(_.dataset).map { case (k,v) => (ddsByDs(k),v.toSeq) }.toMap
      new Cover(bestValsByDs.keys.toSeq, bestValsByDs.keys.map(_candidates.indexOf(_)).toSeq, bestValsByDs.values.toSeq)
    })

  }
}

abstract class TopKGroupingSetCoverer(k:Int, _entities: Seq[Int], _candidates: Seq[DrilledDataset]) extends SetCoverer(k, _entities, _candidates) {

  def name = "TopKGroupingSetCoverer"
  override def toString = s"$name"

  override def covers(): Seq[Cover] = {
    val ddsByDs = _candidates.map(dds => (dds.dataset, dds)).toMap
    val groupsByEntity:Map[Int,Seq[DrilledValue]] = _candidates.flatMap(_.values).groupBy(_.forIndex).toMap
    val sortedGroups = groupsByEntity.mapValues(_.sortBy(v => ddsByDs(v.dataset).inherentScore).reverse)
    (0 until k).map(i => {
      val bestVals = sortedGroups.values.map(group => group(i%group.size))
      val bestValsByDs = bestVals.groupBy(_.dataset).map { case (k,v) => (ddsByDs(k),v.toSeq) }.toMap
      new Cover(bestValsByDs.keys.toSeq, bestValsByDs.keys.map(_candidates.indexOf(_)).toSeq, bestValsByDs.values.toSeq)
    })

  }
}
