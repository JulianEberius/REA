package rea.analysis

import java.util.regex.Pattern
import scala.annotation.tailrec
import scala.collection.mutable
import rea.definitions._

sealed trait DataType { def Specificity: Int }
case object NONE extends DataType { val Specificity = -1 }
case object STRING extends DataType { val Specificity = 0 }
case object EMAIL extends DataType { val Specificity = 1 }
case object URL extends DataType { val Specificity = 2 }
case object DATETIME extends DataType { val Specificity = 3 }
case object DOUBLE extends DataType { val Specificity = 4 }
case object LONG extends DataType { val Specificity = 6 }
case object INTEGER extends DataType { val Specificity = 7 }
case object CURRENCY extends DataType { val Specificity = 8 }

object DataTypes {
    class TypeCheckResult
    case class NoMatch() extends TypeCheckResult
    case class TypeMatches(dataType:DataType) extends TypeCheckResult
    type TypeChecker = (String) => TypeCheckResult

    val TYPE_MAJORITY_THRESHOLD = 0.4
    val NUMERIC_PERCENTAGE = 0.5
    val NUMERIC_MAJORITY_THRESHOLD = 0.5

    val isCurrency = Pattern.compile("-?\\s*([$£₤]\\s*[\\d,]+(\\.\\d+)?)|([\\d,]+(\\.\\d+)?\\s*[€])")
    val isInt= Pattern.compile("(^|[^\\d])-?[\\d,]{1,9}(\\s)?(%)?($|[^\\d])")
    val isLong = Pattern.compile("-?[\\d,]+")
    val isDouble = Pattern.compile("-?[\\d,]+\\.\\d+(\\s)?(%)?")

    val formatCheckers = List[TypeChecker](
        (s:String) => if (s.equals("")) TypeMatches(NONE) else NoMatch(),
        (s:String) => {
            val matcher = isCurrency.matcher(s)
            if (matcher.find && (matcher.end - matcher.start) / s.size.toDouble > NUMERIC_PERCENTAGE)
                TypeMatches(CURRENCY)
            else NoMatch()
        },
        (s:String) => {
            val matcher = isDouble.matcher(s)
            if (matcher.find && (matcher.end - matcher.start) / s.size.toDouble > NUMERIC_PERCENTAGE)
                TypeMatches(DOUBLE)
            else NoMatch()
        },
        (s:String) => {
            val matcher = isInt.matcher(s)
            if (matcher.find && (matcher.end - matcher.start) / s.size.toDouble > NUMERIC_PERCENTAGE)
                TypeMatches(INTEGER)
            else NoMatch()
        },
        (s:String) => {
            val matcher = isLong.matcher(s)
            if (matcher.find && (matcher.end - matcher.start) / s.size.toDouble > NUMERIC_PERCENTAGE)
                TypeMatches(LONG)
            else NoMatch()
        }
        )

    @tailrec
    def applyCheckers(data: String, checkers:Seq[TypeChecker]):DataType =
        checkers match {
            case fc :: rest => fc(data) match {
                case TypeMatches(t) => t
                case NoMatch() => applyCheckers(data, rest)
            }
            case Nil => STRING
        }

    def typeOf(s:String) = applyCheckers(s, formatCheckers)

    def typeCounts(column: Seq[String]) =
        column.map(typeOf).groupBy(identity).mapValues(_.size)

    def columnTypeStrict(column: Seq[String]):DataType = {
        val counts = typeCounts(column)
        if (counts.size == 1)
            return counts.keys.head
        else
            return STRING
    }

    def columnType(column: Seq[String]):DataType = {
        val counts = typeCounts(column).filterKeys(_!=NONE)
        if (counts.size == 1)
            return counts.keys.head
        else if (counts.size == 0)
            return NONE
        val mostFrequent = counts.values.toSeq.sorted.takeRight(2).map(_.toFloat / column.size)
        if ((mostFrequent(1) - mostFrequent(0)) > TYPE_MAJORITY_THRESHOLD)
            return counts.maxBy(_._2)._1

        val cntNumeric = counts.foldLeft[Int](0)((r, tup) =>
            if (tup._1.Specificity >= DOUBLE.Specificity)
                r + tup._2 else r
            )
        if ((cntNumeric.toFloat / column.size) > NUMERIC_MAJORITY_THRESHOLD) // 90% are numeric
            return counts.keys.filter(_.Specificity >= DOUBLE.Specificity).minBy(_.Specificity)
        return STRING
    }



    val toDouble = Pattern.compile("-?[\\d,]+\\.?\\d*")
    def coerceToNumber(v: String): Option[Double] = {
        // val s = v.replaceAll("[^\\d\\.-]+", "")

        val matcher = toDouble.matcher(v)
        val numbers = mutable.ArrayBuffer[String]()
        while (matcher.find())
        	numbers += matcher.group()
        if (numbers.isEmpty)
            return None
        val s = numbers.maxBy(_.size)
        if (s.length < v.length.toFloat * 0.5 && !v.startsWith(s))
            return None
        try {
            Some(s.replaceAll("[^\\d\\.-]+", "").toDouble)
        } catch {
            case _: Throwable => {
                None
            }
        }
    }
}
