package com.spark.app.models

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import java.text.SimpleDateFormat
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import java.sql.Timestamp
import org.apache.spark.broadcast.Broadcast

@SerialVersionUID(123L)
case class Transaction
(
  val period : Option[Timestamp],
  val time : Option[Timestamp],
  val weekDay : Int,
  val clientId : Int,
  val mcc : Int,
  val mccCategory : String,
  val channel : String,
  val currency : Int,
  val amount : Double,
  val category : String,
  var transactionWeek : Option[Int],
  var clientFirstPeriod : Option[Timestamp],
  var clientFirstTransactionDate : Option[Timestamp],
  var transactionDay : Option[Int],
  var periodNumber : Option[Int],
  var targetFlag : Double,
  var targetAmount : Double
) extends Serializable
{
  override def toString = f"""Transaction([time=$time],[clientId=$clientId],[mcc=$mcc],[channel=$channel],[currency=$currency],[amount=$amount],[category=$category])"""
}

@SerialVersionUID(123L)
case class TrainTransaction
(
   val t : Transaction,
   val targetFlag : Double,
   val targetAmount : Double
) extends Serializable
{
  override def toString = f"""($t,[target_flag=$targetFlag],[target_amount=$targetAmount])"""
}


object Transaction {
  
  val maxTransactionDay = 93
  
  def getNearestDay(days : Map[Int, Double], startDay : Int) : Double = {
	for (i <- startDay to 0 by -1) {
		if (days.contains(i)) {
			return days.get(i).get
		}
	}

	throw new Exception(f"cant find the day $startDay")
  }

  def parse(raw : Array[String], currencyMap : Broadcast[Map[Int, Map[Int, Double]]], mccDict : Broadcast[Map[Int, String]]) : Transaction = {
	  val period = {
      try  {
        val strDate = raw(0).toLowerCase
        val time = DateTime.parse(strDate, DateTimeFormat.forPattern("dd/MM/yyyy"))
        Some(new java.sql.Timestamp(time.getMillis()))
      } catch {
        case e : java.lang.UnsupportedOperationException => None
        case e : java.lang.IllegalArgumentException => None
      } finally {
        None
      }
    }

    val time = {
      try  {
        val strDate = raw(5).toLowerCase
        val date = strDate.substring(0, 2) + strDate.charAt(2).toUpper + strDate.substring(3)
        val time = DateTime.parse(date, DateTimeFormat.forPattern("ddMMMyy:HH:mm:ss"))
		
        Some(new java.sql.Timestamp(time.getMillis()))
      } catch {
        case e : java.lang.UnsupportedOperationException => None
        case e : java.lang.IllegalArgumentException => None
      } finally {
        None
      }
    }

    val dayNumber = {
      if (time.isDefined)
        Some((time.get.getTime().toDouble / (1000 * 24 * 3600)).floor.toInt)
      else None
    }

    val weekDay = {
      if (time.isDefined)
        time.get.toLocalDateTime.getDayOfWeek.getValue
      else {
        0
      }
    }
	
    val clientId = raw(1).toInt
    val mcc = raw(2).toInt

    val mccCategory = mccDict.value.get(mcc).get
    val channel = raw(3)
    val currency = raw(4).toInt
    val amount = {
      val rate =
        if (currencyMap.value.contains(currency) && dayNumber.isDefined) {
          getNearestDay(currencyMap.value.get(currency).get, dayNumber.get)
        }
        else 1.0
      rate * raw(6).toDouble
    }

    val category = raw(7)
    new Transaction(period, time, weekDay, clientId, mcc, mccCategory, channel, currency, amount, category, None, None, None, None, None, 0.0, 0.0)
  }

  def parseTarget(raw : Array[String], t : Transaction) : Transaction = {
    val targetFlag = if (raw.size > 8) raw(8).toDouble else 0.0
    val targetAmount = if (raw.size > 9) raw(9).toDouble else 0.0

    t.targetAmount = targetAmount
    t.targetFlag = targetFlag
    t
  }


  val indexMap = Map(
    "transactionWeek" -> "transactionWeekIndex",
    "periodNumber" -> "periodNumberIndex",
    //"transactionDay" -> "transactionDayIndex",
    "mccCategory" -> "mccIndex",
    "weekDay" -> "weekDayIndex",
    "channel" -> "channelIndex",
    "currency" -> "currencyIndex",
    "category" -> "categoryIndex"
  )

  val oneHotCols = Map(
    "transactionWeekIndex" -> "transactionWeekVec",
    "periodNumberIndex" -> "periodNumberVec",
    //"transactionDayIndex" -> "transactionDayVec",
    "mccIndex" -> "mccVec",
    "weekDayIndex" -> "weekDayVec",
    "channelIndex" -> "channelVec",
    "currencyIndex" ->  "currencyVec",
    "categoryIndex" -> "categoryVec"
  )

  val aggCols : Seq[String] = (oneHotCols.values.toArray[String] ++ Array("amount")).toSeq
}
