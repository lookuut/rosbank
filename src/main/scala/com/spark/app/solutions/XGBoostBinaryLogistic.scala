package com.spark.app.solutions

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import org.joda.time.DateTime

import org.apache.spark.rdd.RDD

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator, CrossValidatorModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

import ml.dmlc.xgboost4j.scala.spark.{XGBoostEstimator, XGBoostClassificationModel}

import scala.collection.mutable.HashMap

import com.spark.app.SparkApp

class XGBoostBinaryLogistic(private val spark : SparkSession) {
	
  import spark.implicits._

  private val trainDataPart = 1.0

  def train (
              trainData : RDD[(String, Double, Vector)],
              lambda : Double
            ) : CrossValidatorModel = {

    val trainDF = trainData.toDF("customer_id", "label", "features")

    val xgb = new XGBoostEstimator(get_param().toMap).
      setLabelCol("label").
      setFeaturesCol("features")
    // XGBoost paramater grid

    val xgbParamGrid = (new ParamGridBuilder()
      .addGrid(xgb.round, Array(100))
      .addGrid(xgb.maxDepth, Array(10))
      .addGrid(xgb.maxBins, Array(20))
      .addGrid(xgb.minChildWeight, Array(0.2))
      .addGrid(xgb.alpha, Array(0.8))
      .addGrid(xgb.lambda, Array(lambda))
      .addGrid(xgb.subSample, Array(0.65))
      .addGrid(xgb.eta, Array(0.006))
      .build())

    val pipeline = new Pipeline().setStages(Array(xgb))

    val evaluator = (new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC"))

    val cv = (new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(xgbParamGrid)
      .setNumFolds(2)
      )

    val model = cv.fit(trainDF)

    model.save(SparkApp.modelsDir + f"xgboost-lambda:$lambda" + SparkApp.dateFormat.print(DateTime.now))
    model
  }

  def get_param(): HashMap[String, Any] = {
    val params = new HashMap[String, Any]()
    params += "eta" -> 0.1
    params += "max_depth" -> 8
    params += "gamma" -> 0.0
    params += "colsample_bylevel" -> 1
    params += "objective" -> "binary:logistic"
    params += "num_class" -> 2
    params += "booster" -> "dart"
    params += "num_rounds" -> 20
    params += "nWorkers" -> 3
    return params
  }

  def generateModel(trainData : RDD[(Int, Double, Vector)]) : String = {

    val trainFeaturesDF = trainData.
      toDF("clientId", "label", "features")
    val Array(trainDF, testDF) = trainFeaturesDF.randomSplit(Array(trainDataPart, 1 - trainDataPart))

    val xgb = new XGBoostEstimator(get_param().toMap).
      setLabelCol("label").
      setFeaturesCol("features")
    // XGBoost paramater grid

    val xgbParamGrid = (new ParamGridBuilder()
      .addGrid(xgb.round, Array(2000))
      .addGrid(xgb.maxDepth, Array(3))
      .addGrid(xgb.maxBins, Array(2))
      .addGrid(xgb.gamma, Array(0.0))
      .addGrid(xgb.minChildWeight, Array(1.0, 3.0))
      .addGrid(xgb.alpha, Array(0.6))//checked
      .addGrid(xgb.lambda, Array(0.8))//checked
      .addGrid(xgb.subSample, Array(0.5))//checked
      .addGrid(xgb.eta, Array(0.001))
      .build())

    val pipeline = new Pipeline().setStages(Array(xgb))

    val evaluator = (new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC"))

    val cv = (new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(xgbParamGrid)
      .setNumFolds(10)
      )

    val xgbModel = cv.fit(trainDF)
    val bestModelPath = SparkApp.modelsDir + f"xgboost-" + SparkApp.dateFormat.print(DateTime.now)
    xgbModel.save(bestModelPath)
   
    val cvResult = xgbModel.transform(testDF)
    val trainResult = xgbModel.transform(trainDF)
 
    println("CV data percent ===>" + evaluator.evaluate(cvResult) + " train data percent " + evaluator.evaluate(trainResult))
	
    (xgbModel.bestModel.asInstanceOf[PipelineModel]
      .stages(0).asInstanceOf[XGBoostClassificationModel]
      .extractParamMap().toSeq.foreach(println))
		
    bestModelPath
  }

  def prediction(
                  toPredictTransactions : RDD[(Int, Double, Vector)],
                  model : CrossValidatorModel
                ) : RDD[(Int, Double)] = {


    val testDF = toPredictTransactions.
      map(t => (t._1, t._3)).
      toDF("id", "features").
      cache


    val results = model.transform(testDF)
    results.
      select("id", "prediction").
      map {
        case Row(
        id : Int,
        prediction : Double
        ) =>
          (id, prediction)
      }.
      rdd
  }

  def loadModel(path : String) : CrossValidatorModel = {
    CrossValidatorModel.load(path)
  }
}
