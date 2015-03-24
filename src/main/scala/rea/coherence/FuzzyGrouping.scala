package rea.coherence

import scala.collection.JavaConversions._
import rea.definitions._
import org.apache.commons.math3.ml.clustering.DBSCANClusterer
import org.apache.commons.math3.ml.clustering.Clusterable
import org.apache.commons.math3.ml.clustering.Cluster
import scala.math.log10

class FuzzyGrouping(val entities: Set[Int], val datasets: Seq[DrilledDataset]) {

  class ClusterValue(val original:DrilledValue) extends Clusterable {
    val point = original.value match {
      case d:java.lang.Double => Array(log10(d))
      case s:String => Array(s.size.toDouble)
      case _ => Array(0.0)
    }
    override def getPoint = point
  }

  val clusterer = new DBSCANClusterer[ClusterValue](0.1, 0)

  def cluster(): Map[Int, Seq[Seq[DrilledValue]]] = {
    val valuesByEntity = datasets.flatMap(_.values).groupBy(_.forIndex).mapValues(_.map(new ClusterValue(_)))
    valuesByEntity.mapValues(vals =>
      clusterer.cluster(vals).map(cluster => cluster.getPoints().map(_.original))
    )
  }
}
