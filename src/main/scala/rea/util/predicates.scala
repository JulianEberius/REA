package rea.util

import scala.util.Random

abstract class Predicate() {
  def evaluate(obj: Any): Boolean = true

  def discriminates(objs: Iterable[Any]): Boolean = {
    val sel = selectivity(objs)
    sel != 0.0 && sel != 1.0
  }

  def applicable(objs: Iterable[Any]): Boolean = true

  def applicableForValue(obj: Any): Boolean = true

  def selectivity(objs: Iterable[Any]): Double =
    objs.filter(evaluate).size.toDouble / objs.size.toDouble

}
trait NumericPredicate extends Predicate {

  override def applicable(objs: Iterable[Any]): Boolean =
    objs.forall(_ match {
      case n : Double => true
      case _ => false
    })
    override def applicableForValue(obj: Any): Boolean = obj match {
      case n : Double => true
      case _ => false
    }

}
case class IsNumericPredicate() extends Predicate with NumericPredicate {

  override def evaluate(obj: Any) = obj match {
    case d: Double => true
    case d: java.lang.Double => true
    case _ => false
  }

  override def discriminates(objs: Iterable[Any]): Boolean = true
}

trait NumericValuedPredicate extends NumericPredicate {
  def value: Double

  // randomized API
  def rnd(e:String, seedModifier:Int) = new Random(e.hashCode()*10000000 + seedModifier*1000000).nextDouble
  def selectRandom(e:String, sel:Double, seedModifier:Int) = rnd(e, seedModifier) < sel
  def selectRandom(e:String, sel:Double) = rnd(e, 0) < sel
  def randomAcceptedValue(e:String, sel:Double, seedModifier:Int, fuzziness:Double=0.0): java.lang.Double
  def randomFilteredValue(e:String, sel:Double, seedModifier:Int, fuzziness:Double=0.0): java.lang.Double

  def randomPick(es:Seq[String], seedModifier:Int) =
    es((new Random(seedModifier*1000000).nextDouble * es.size).toInt)

  def artificial_values(entities:Seq[String], sel:Double, seedModifier:Int, fuzziness:Double=0.0):Array[java.lang.Double] = {
    // always accept at least one value (sel is never truly 0.0)
    // val firstValue = entities.take(1).map(e => randomAcceptedValue(e, sel, seedModifier))

    val minAcceptedEntity = randomPick(entities, seedModifier)

    // the others are randomized with prob_accept = sel
    entities.map(e =>
        if ((e == minAcceptedEntity) || selectRandom(e, sel))
        // if (selectRandom(e, sel, seedModifier))
          randomAcceptedValue(e, sel, seedModifier, fuzziness)
        else
          randomFilteredValue(e, sel, seedModifier, fuzziness)
      ).toArray[java.lang.Double]
//
    // return (firstValue ++ otherValues).toArray[java.lang.Double]
  }
}

case class LTPredicate(predicateValue: Double) extends Predicate with NumericValuedPredicate {
  override def value: Double = predicateValue
  override def evaluate(obj: Any) = obj match {
    case d: Double => d < predicateValue
    case _ => super.evaluate(obj)
  }

  override def randomAcceptedValue(e:String, sel:Double, seedModifier:Int, fuzziness:Double=0.0)
    = new java.lang.Double(value * rnd(e, seedModifier) + (value*fuzziness*(1.0-sel)))
  override def randomFilteredValue(e:String, sel:Double, seedModifier:Int, fuzziness:Double=0.0)
    = new java.lang.Double(value + value * rnd(e, seedModifier) - (value*fuzziness*sel))

  override def toString = s"<$value"
}
case class GTPredicate(predicateValue: Double) extends Predicate with NumericValuedPredicate {
  override def value: Double = predicateValue
  override def evaluate(obj: Any) = obj match {
    case d: Double => d > predicateValue
    case _ => super.evaluate(obj)
  }

  override def randomAcceptedValue(e:String, sel:Double, seedModifier:Int, fuzziness:Double=0.0)
    = new java.lang.Double(value + value * rnd(e, seedModifier) - (value*fuzziness*(1.0-sel)))
  override def randomFilteredValue(e:String, sel:Double, seedModifier:Int, fuzziness:Double=0.0)
    = new java.lang.Double(value * rnd(e, seedModifier) + (value*fuzziness*sel))

  override def toString = s">$value"
}
case class NoPredicate() extends Predicate

object Predicate {
  def fromString(s: String): Predicate = {
    val st = s.trim()
    extractDoubleValue(st.drop(1)) match {
      case Some(d) => st match {
        case st if st.startsWith("<") => LTPredicate(d)
        case st if st.startsWith(">") => GTPredicate(d)
        case _ => NoPredicate()
      }
      case None => st match {
        case st if st.startsWith("isNumeric") => IsNumericPredicate()
        case _ => NoPredicate()
      }
    }
  }

  def extractDoubleValue(s: String): Option[Double] =
    try { new Some(s.toDouble) }
    catch { case _: Throwable => None }

}