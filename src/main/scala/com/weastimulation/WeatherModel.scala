package com.weastimulation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LinearRegressionModel
import java.io._
import org.apache.commons.io.FileUtils



/**
  * Created by validboy on 8/28/16.
  * The Model training and building Object
  */
object WeatherModel {

  def deleteDirectory(path: String): Unit = {
    val directory = new File(path)
    if(directory.exists()) try {
      FileUtils.deleteDirectory(directory)
    }
    catch {
      case e: IOException => e.getMessage
    }
  }

  def saveModels(
                  sc: SparkContext,
                  temperatureModel: LinearRegressionModel,
                  airPressureModel: LinearRegressionModel,
                  humidityModel: LinearRegressionModel,
                  weatherConditionModel: DecisionTreeModel
                ): Unit = {
    deleteDirectory("src/main/resources/temperatureModel")
    temperatureModel.save(sc,"src/main/resources/temperatureModel")

    deleteDirectory("src/main/resources/airPressureModel")
    airPressureModel.save(sc,"src/main/resources/airPressureModel")

    deleteDirectory("src/main/resources/humidityModel")
    humidityModel.save(sc,"src/main/resources/humidityModel")

    deleteDirectory("src/main/resources/weatherConditionModel")
    weatherConditionModel.save(sc,"src/main/resources/weatherConditionModel")
  }

  def buildModels() = {

    val sc = new SparkContext(new SparkConf().setAppName("Weather Model Stimulation").setMaster("local"))
    ////
    val obsDataLines = sc.textFile("src/main/resources/observationData.txt")

    val obsDataLine = obsDataLines map {line => line.split("\\|", -1) }

    val obsDataToWeatherData = obsDataLine map {r =>
      new WeatherData(r(3),r(1).toDouble,r(2).toDouble,r(0),r(4).toInt,r(5).toDouble,r(6).toDouble,r(7).toInt)
    }

    val splits = obsDataToWeatherData.randomSplit(Array(0.7, 0.3), 13L)

    val (trainingSet, _) = (splits(0), splits(1))

    /*
   * Temperature regression Model
   * Including below features
   * timeSeriesFeature: MMDDHHMI
   * seasonFeature: category Spring/Summer/Autumn/Winter
   * dayNightFeature: category day time or night time
   * latitudeFeature: latitude
   * longitudeFeature: longitude
   * elevationFeature: elevation
   * regionFeature: category Tropics/Subtropics/Temperate zone/Frigid zone
   * oceanologyFeature: category Warm current/Cold current/Inland
   */
    lazy val temperatureVector = trainingSet map (t =>
      LabeledPoint(t.temperatureLabel, t.temperatureVector))

    temperatureVector.cache()
    temperatureVector.count()

    val temperatureModel = LinearRegressionWithSGD.train(temperatureVector, 100, 0.00001)

    temperatureVector.unpersist()

    /*
   * air pressure regression Model
   * Including below features
   * timeSeriesFeature: MMDDHHMI
   * regionFeature: category Tropics/Subtropics/Temperate zone/Frigid zone
   * latitudeFeature: latitude
   * longitudeFeature: longitude
   * elevationFeature: elevation
   * temperatureFeature: temperature
   */
    lazy val airPressureVector = trainingSet map (t =>
      LabeledPoint(t.airPressureLabel, t.airPressureVector))

    airPressureVector.cache()
    airPressureVector.count()

    val airPressureModel = LinearRegressionWithSGD.train(airPressureVector, 100, 0.0001)

    airPressureVector.unpersist()

    /*
   * humidity regression Model
   * Including below features
   * timeSeriesFeature: MMDDHHMI
   * seasonFeature: category Spring/Summer/Autumn/Winter
   * oceanologyFeature: category Warm current/Cold current/Inland
   * latitudeFeature: latitude
   * longitudeFeature: longitude
   * elevationFeature: elevation
   */

    lazy val humidityVector = trainingSet map (t =>
      LabeledPoint(t.humidityLabel, t.humidityVector))

    humidityVector.cache()
    humidityVector.count()

    val humidityModel = LinearRegressionWithSGD.train(humidityVector, 100, 0.0001)

    humidityVector.unpersist()


    /*
   * weatherCondition classification Model
   * Including below features
   * timeSeriesFeature: MMDDHHMI
   * seasonFeature: category Spring/Summer/Autumn/Winter
   * oceanologyFeature: category Warm current/Cold current/Inland
   * latitudeFeature: latitude
   * longitudeFeature: longitude
   * elevationFeature: elevation
   * temperatureFeature: temperature
   * airPressureFeature: airPressure
   * humidityFeature: humidity
   */
    lazy val weatherConditionVector = trainingSet map (t =>
      LabeledPoint(t.weatherConditionLabel, t.weatherConditionVector))

    weatherConditionVector.cache()
    weatherConditionVector.count()

    val numClasses = 3 // Sunny/Rain/Snow
    val weatherConditionCateFeaturesInfo = Map[Int, Int]()
    val weatherConditionImpurity = "gini"
    val weatherConditionMaxDepth = 4
    val weatherConditionMaxBins = 32
    val weatherConditionModel = DecisionTree.trainClassifier(weatherConditionVector,
      numClasses, weatherConditionCateFeaturesInfo, weatherConditionImpurity, weatherConditionMaxDepth, weatherConditionMaxBins)

    weatherConditionVector.unpersist()

    this.saveModels(sc,temperatureModel,airPressureModel,humidityModel,weatherConditionModel)

    sc.stop()

  }

  def main(args: Array[String]) {

    buildModels()

  }

}
