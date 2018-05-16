
package com.spark.app.models

import org.joda.time.format.DateTimeFormat
import java.sql.Timestamp
import org.apache.spark.rdd.RDD

case class Currency (
	val cType : Int,
	val date : Timestamp,
	val rate : Double
)
{
	override def toString = f"""Currency $cType $date $rate"""
}


object Currency {

  val dtf = DateTimeFormat.forPattern("yyyy-MM-dd")

  def parse(rateRdd : RDD[Array[String]]) : Map[Int, Double] = {
		rateRdd.map{
			case raw =>
				val day = (dtf.parseDateTime(raw(1)).toDate().getTime().toDouble / (1000 * 24 * 3600)).floor.toInt
				(day, raw(3).toDouble)
		}.collect.toMap
  }
}
