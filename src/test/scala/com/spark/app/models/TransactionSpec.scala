package com.spark.app.models

import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


object TransactionSpec extends org.specs2.mutable.Specification {
 "Parse test" should {
    "Test parser" in {

      val strDate = "18APR17:00:00:00".toLowerCase
      val date = strDate.substring(0, 2) + strDate.charAt(2).toUpper + strDate.substring(3)
      println(date)
      println(DateTime.parse(date, DateTimeFormat.forPattern("ddMMMyy:HH:mm:ss")))

      1 > 0
    }
  }
}