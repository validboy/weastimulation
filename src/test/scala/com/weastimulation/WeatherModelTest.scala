package com.weastimulation

import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * Created by validboy on 8/30/16.
  * Model and Prediction Test
  * The test cases will compare the prediction with
  * the actual observation value, the prediction will
  * at least require 35% accuracy to pass.
  */
@RunWith(classOf[JUnitRunner])
class WeatherModelTest extends FunSuite {

  test("Test 3: Model and Prediction Error Test") {

    val sc = new SparkContext(new SparkConf().setAppName("Test 3: Model and Prediction Test").setMaster("local"))

    val temperatureModelPath = "src/main/resources/temperatureModel"
    val airPressureModelPath = "src/main/resources/airPressureModel"
    val humidityModelPath = "src/main/resources/humidityModel"
    val weatherConditionModelPath = "src/main/resources/weatherConditionModel"

    val temperatureModel = LinearRegressionModel.load(sc, temperatureModelPath)
    val airPressureModel = LinearRegressionModel.load(sc, airPressureModelPath)
    val humidityModel = LinearRegressionModel.load(sc, humidityModelPath)
    val weatherConditionModel = DecisionTreeModel.load(sc, weatherConditionModelPath)


    val obsDataLines = sc.textFile("src/main/resources/observationData.txt")

    val obsDataLine = obsDataLines map {line => line.split("\\|", -1) }

    val obsDataToWeatherData = obsDataLine map {r =>
      new WeatherData(r(3),r(1).toDouble,r(2).toDouble,r(0),r(4).toInt,r(5).toDouble,r(6).toDouble,r(7).toInt)
    }

    val splits = obsDataToWeatherData.randomSplit(Array(0.7, 0.3), 13L)

    val (_, testingSet) = (splits(0), splits(1))

    testingSet.cache()

    val testSetCount = testingSet.count()
    val tempLabelAndPreds = testingSet map {t =>
      val temperaturePreds = temperatureModel.predict(t.temperatureVector)
      (t.temperatureLabel,temperaturePreds)
    }

    val tempPredsError = tempLabelAndPreds filter(r => Math.abs(r._1 - r._2) >= 6)
    println("TestSet count: " + testSetCount)
    val tempPredsCount: Double = tempLabelAndPreds.count()
    val tempErrorcount = tempPredsError.count()
    val tempPrecentage = (tempPredsCount - tempErrorcount) / tempPredsCount
    println("Temperature Predict output count: " + tempPredsCount)
    println("Temperature Predict Errors count: " + tempErrorcount)
    println("Temperature Predict correct prediction %: " + tempPrecentage)

    assert(tempPrecentage >= 0.35)

    val airPressureLabelAndPreds = testingSet map { t =>
      val airPressurePreds = airPressureModel.predict(t.airPressureVector)
      (t.airPressureLabel, airPressurePreds)
    }

    val airPressurePredsError = airPressureLabelAndPreds filter(r => Math.abs(r._1 - r._2) >= 50)
    println("TestSet count: " + testSetCount)
    val airPressurePredsCount: Double = airPressureLabelAndPreds.count()
    val airPressureErrorcount = airPressurePredsError.count()
    val airPressurePrecentage = (airPressurePredsCount - airPressureErrorcount) / airPressurePredsCount
    println("Pressure Predict output count: " + airPressurePredsCount)
    println("Pressure Predict Errors count: " + airPressureErrorcount)
    println("Pressure Predict correct prediction %: " + airPressurePrecentage)

    assert(airPressurePrecentage >= 0.35)

    val humidityLabelAndPreds = testingSet map { t =>
      val humidityPreds = humidityModel.predict(t.humidityVector)
      (t.humidityLabel, humidityPreds)
    }

    val humidityPredsError = humidityLabelAndPreds filter(r => Math.abs(r._1 - r._2) >= 25)
    println("TestSet count: " + testSetCount)
    val humidityPredsCount: Double = humidityLabelAndPreds.count()
    val humidityErrorcount = humidityPredsError.count()
    val humidityPrecentage = (humidityPredsCount - humidityErrorcount) / humidityPredsCount
    println("Humidity Predict output count: " + humidityPredsCount)
    println("Humidity Predict Errors count: " + humidityErrorcount)
    println("Humidity Predict correct prediction %: " + humidityPrecentage)

    assert(humidityPrecentage >= 0.35)

    val weatherConditionLabelAndPreds = testingSet map { t =>
      val weatherConditionPreds = weatherConditionModel.predict(t.weatherConditionVector)
      (t.weatherConditionLabel, weatherConditionPreds)
    }

    val weatherConditionPredsError = weatherConditionLabelAndPreds filter(r=> r._1 != r._2)
    println("TestSet count: " + testSetCount)
    val weatherConditionPredsCount: Double = weatherConditionLabelAndPreds.count()
    val weatherConditionErrorcount = weatherConditionPredsError.count()
    val weatherConditionPrecentage = (weatherConditionPredsCount - weatherConditionErrorcount) / weatherConditionPredsCount
    println("weather Condition Predict output count: " + weatherConditionPredsCount)
    println("weather Condition Predict Errors count: " + weatherConditionErrorcount)
    println("weather Condition Predict correct prediction %: " + weatherConditionPrecentage)

    assert(weatherConditionPrecentage >= 0.35)

    sc.stop()

  }

}
