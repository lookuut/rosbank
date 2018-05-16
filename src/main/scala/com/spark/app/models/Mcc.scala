package com.spark.app.models

import org.apache.spark.rdd.RDD

object Mcc {
  def parse(rateRdd : RDD[Array[String]]) : Map[Int, String] = {
    rateRdd.map{
      case raw =>
        (raw(0).toInt, raw(2))
    }.collect.toMap
  }
}
