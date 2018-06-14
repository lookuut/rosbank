package com.spark.app


import com.spark.app.models.{Currency, Mcc, Transaction}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.spark.app.solutions.XGBoostBinaryLogistic
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.broadcast.Broadcast

object SparkApp {

  val spark = SparkSession.builder().getOrCreate()
  val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd-hh-mm-ss")

  import spark.implicits._

  val resourceDir = Config.appDir + "resource/"
  val modelsDir = resourceDir + "models/"

  private val predictionsDir = resourceDir + "predictions/"

  private val testDataFile = resourceDir + "test.csv"
  private val trainDataFile = resourceDir + "train.csv"
  
  private val usdCurrencyFile = resourceDir + "USD_RUR.csv"
  private val eurCurrencyFile = resourceDir + "EUR_RUR.csv"

  private val usdCurrencyCode = 840
  private val eurCurrencyCode = 978

  private val mccDictFile = resourceDir + "mcc-codes.csv"
  
  def main(args: Array[String]) {

    var (train, test) = loadData(trainDataFile, testDataFile)

	  var unionedData = train.union(test)

    val indexStage = indexer(Transaction.indexMap)


    val pipeline = new Pipeline().setStages(indexStage.toArray)

    val indexModel = pipeline.fit(unionedData)

    train = indexModel.transform(train)
    test = indexModel.transform(test)

    val oneHotPipeline = new Pipeline().setStages(oneHot(Transaction.oneHotCols).toArray)

    unionedData = train.union(test)

    val oneHotModel = oneHotPipeline.fit(unionedData)

    train = oneHotModel.transform(train)
    test = oneHotModel.transform(test)
	
    train = train.drop(Transaction.indexMap.keys.toArray:_*)
    test = test.drop(Transaction.indexMap.keys.toArray:_*)

    train = train.drop(Transaction.oneHotCols.keys.toArray:_*)
    test = test.drop(Transaction.oneHotCols.keys.toArray:_*)
	
    train = train.drop("time", "period", "mcc", "clientFirstPeriod", "transactionDay" , "clientFirstTransactionDate")
    test = test.drop("time", "period", "mcc", "clientFirstPeriod", "transactionDay" , "clientFirstTransactionDate")
    train.show(10)
    val trainRDD = prepare(train).map(t => (t._1, t._2, t._4)).persist()
    val testRDD = prepare(test).map(t => (t._1, t._2, t._4)).persist()
	
    binaryLogistic(trainRDD, testRDD)
  }

  def prepare(data : DataFrame) : RDD[(Int, Double, Double, Vector)] = {
    data.rdd.map{
      case Row(
	    clientId : Int,
      amount : Double,

      targetFlag: Double,
      targetSum: Double,

      weekDayVec : Vector,
      currencyVec : Vector,
      mccVec : Vector,
      categoryVec : Vector,
      transactionWeekVec : Vector,
      channelVec : Vector,
      periodNumberVec : Vector
      ) =>
        (clientId, (targetFlag, targetSum, amount, channelVec, transactionWeekVec, periodNumberVec, weekDayVec, currencyVec, mccVec, categoryVec))
    }.
      groupByKey.
      map {
        case (clientId, data) =>
			
          val clientAmountSum = data.map(_._3).sum
          val propFunc = (amount : Double, vecVal : Double) => (amount / clientAmountSum) * vecVal
          val sumFunc = (amount : Double, vecVal : Double) => (amount) * vecVal

          val transactionWeekCollapsedVec = for (i <- 0 until data.head._5.size) yield {
                data.map(t => sumFunc(t._3, t._5(i))).reduce(_ + _)
              }

          val periodCollapsedVec = for (i <- 0 until data.head._6.size) yield {
                data.map(t => sumFunc(t._3, t._6(i))).reduce(_ + _)
              }

          val weekDayCollapsedVec = for (i <- 0 until data.head._7.size) yield {
            data.map(t => propFunc(t._3, t._7(i))).reduce(_ + _)
          }

          val currencyCollapsedVec = for (i <- 0 until data.head._8.size) yield {
            data.map(t => propFunc(t._3, t._8(i))).reduce(_ + _)
          }

          val mccCollapsedVec = for (i <- 0 until data.head._9.size) yield {
                data.map(t => propFunc(t._3, t._9(i))).reduce(_ + _)
              }

          val categoryCollapsedVec = for (i <- 0 until data.head._10.size) yield {
                data.map(t => propFunc(t._3, t._10(i))).reduce(_ + _)
              }

          val channelVec = data.head._4

          val vec = (
            transactionWeekCollapsedVec
                ++
              periodCollapsedVec
                ++
              weekDayCollapsedVec
                ++
              currencyCollapsedVec
                ++
              mccCollapsedVec
                ++
              categoryCollapsedVec
                ++
              channelVec.toArray
                ++
              Array(clientAmountSum)
            )

          (clientId, data.head._1, data.head._2, org.apache.spark.ml.linalg.Vectors.dense(vec.toArray))
      }
  }


  def recSum[T : Numeric](s : Iterable[Iterable[T]]) : List[T] = {
    val goodVecs = s.filterNot(_.isEmpty)

    if(goodVecs.isEmpty)
      List.empty[T]
    else
      goodVecs.map(_.head).sum :: recSum(goodVecs.map(_.tail))
  }

  def oneHot (cols : Map[String , String]) : Iterable[PipelineStage] = {
    for (col <- cols) yield {
      new OneHotEncoder()
        .setInputCol(col._1)
        .setOutputCol(col._2)
    }
  }

  def indexer(indexMap : Map[String, String]) : Iterable[PipelineStage] = {
    for (col <- indexMap) yield {
      val indexer = new StringIndexer()
      indexer.setInputCol(col._1).setOutputCol(col._2).setHandleInvalid("skip")
      indexer
    }
  }

  def binaryLogistic(train : RDD[(Int, Double, org.apache.spark.ml.linalg.Vector)],
                     test : RDD[(Int, Double, org.apache.spark.ml.linalg.Vector)]) {


    val binaryLogistic = new XGBoostBinaryLogistic(spark)
    val modelPath = binaryLogistic.generateModel(train)

    val model = binaryLogistic.loadModel(modelPath)
    val prediction = binaryLogistic.prediction(test, model)

    val df = prediction.toDF("_ID_", "_VAL_").
      coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(f"${predictionsDir}xgboost-resuls${SparkApp.dateFormat.print(DateTime.now)}")
  }
	
  def loadCurrency() : Broadcast[Map[Int, Map[Int, Double]]] = {
    val filter = (v : String) => !v.contains("TICKER,DATE")
    val split = (line : String) => line.split(",")
    val currencyRate = Map(eurCurrencyCode -> Currency.parse(rddText(eurCurrencyFile, filter, split))) ++ Map(usdCurrencyCode -> Currency.parse(rddText(usdCurrencyFile, filter, split)))
    spark.sparkContext.broadcast(currencyRate)
  }

  def loadMccCodesDict() : Broadcast[Map[Int, String]] = {
    val filter = (v : String) => true
    val split = (line : String) => line.split(",")
    val currencyRate = Mcc.parse(rddText(mccDictFile, filter, split))
    spark.sparkContext.broadcast(currencyRate)
  }

  def rddText(file : String, filter : String => Boolean, split : String => Array[String]) =
      spark.sparkContext.textFile(file).filter(filter).map(split)

  def loadData(trainFile : String, testFile : String) : (DataFrame, DataFrame) = {
	  val currency = loadCurrency()
	  val mccDict = loadMccCodesDict()

    val filter = (v : String) => !v.contains("PERIOD,cl_id,MCC,channel_type")

    val split = (line : String) => line.split(",")

    val trainText = rddText(trainFile, filter, split)
	
    val train = trainText.
      filter(t => t.size > 2).
      map(t => Transaction.parseTarget(t, Transaction.parse(t, currency, mccDict))).
      filter(t => t.time.isDefined)


    val testText = rddText(testFile,filter, split)

      val test = testText.
        filter(t => t.size > 2).
        map(t => Transaction.parse(t, currency, mccDict)).
        filter(t => t.time.isDefined)

    val trainDF = setMinDate(train).filter(t => t.transactionDay.get <= Transaction.maxTransactionDay).toDF()

    val testDF = setMinDate(test).filter(t => t.transactionDay.get <= Transaction.maxTransactionDay).toDF()

    println("Train count ======>" + trainText.count)
    println("Test count ======>" + testText.count)

    (trainDF, testDF)
  }
  
  def setMinDate(data : RDD[Transaction]) : RDD[Transaction] = {
	data.map(t => (t.clientId, t)).
		groupByKey.
		mapValues{
			case cTransactions =>
				val minDate = cTransactions.map(t => (t.time.get, t.time.get.getTime)).minBy(_._2)._1
				val minPeriod = cTransactions.map(t => (t.period.get, t.period.get.getTime)).minBy(_._2)._1

        val minPeriodDateTime = new DateTime(minPeriod.getTime)
        val minPeriodMonth = minPeriodDateTime.year().get() * 12 + minPeriodDateTime.monthOfYear().get()

				cTransactions.map{
					case t => 
            val tDay = ((t.time.get.getTime() - minDate.getTime()).toDouble / (1000 * 24 * 3600)).floor.toInt
            t.transactionDay = Some(tDay)
            t.clientFirstTransactionDate = Some(minDate)
            t.clientFirstPeriod = Some(minPeriod)
            val dateTime = new DateTime(t.period.get.getTime)
            t.periodNumber = Some(dateTime.year().get() * 12 + dateTime.monthOfYear().get() - minPeriodMonth)
            t.transactionWeek = Some((tDay.toDouble / 7).floor.toInt)
          t
				}
		}.
		flatMap(t => t._2)
  }
}
