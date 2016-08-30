package com.weastimulation

import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import java.text.SimpleDateFormat
import java.util.SimpleTimeZone

import scala.util.Random


/**
  * Created by validboy on 8/28/16.
  * The Class provides method to iterate and
  * stimulate the weather prediction on time
  * series
  */
class WeatherStimulation(
                        sc: SparkContext,
                        val outputFilePath: String = "src/main/resources/weatherStimulation.txt",
                        temperatureModelPath: String = "src/main/resources/temperatureModel",
                        airPressureModelPath: String = "src/main/resources/airPressureModel",
                        humidityModelPath: String = "src/main/resources/humidityModel",
                        weatherConditionModelPath: String = "src/main/resources/weatherConditionModel"
                        ) {

  /*
   * Load all the models Temperature/AirPressure/Humidity/WeatherCondition
   */
  val weatherStimulationFile = new PrintWriter(new File(outputFilePath))
  val temperatureModel = LinearRegressionModel.load(sc, temperatureModelPath)
  val airPressureModel = LinearRegressionModel.load(sc, airPressureModelPath)
  val humidityModel = LinearRegressionModel.load(sc, humidityModelPath)
  val weatherConditionModel = DecisionTreeModel.load(sc, weatherConditionModelPath)

  def closeFileStream() = {

    weatherStimulationFile.close()

  }

  def addRandomTimeOnDate(ds: String): String = {

    val hoursChangesLowerBound = 18
    val hoursChangesRange = 6
    val minChangesUpperBound = 59
    val secChangesUpperBound = 59
    val random = Random
    random.setSeed(13L)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
    val date = dateFormat.parse(ds)
    dateFormat.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"))
    // Hours 18-24
    val randomHours = hoursChangesLowerBound + random.nextInt(hoursChangesRange)
    // Minute 0 - 59
    val randomMin = random.nextInt(minChangesUpperBound)
    // Secound 0 - 59
    val randomSec = random.nextInt(secChangesUpperBound)
    date.setTime(date.getTime + randomHours*3600000 + randomMin*60000 + randomSec*1000)
    val dateAsString = dateFormat.format(date)
    dateAsString.replaceAll("\\+00","Z")

  }

  def stimulation(initData: WeatherData, noOfStimulation: Int):Unit = {
    if(noOfStimulation > 0) {

      val temperaturePredict = this.temperatureModel.predict(initData.temperatureVector)

      val airPressurePredict = this.airPressureModel.predict(Vectors.dense(
        initData.airPressureLatitudeFeature,
        initData.airPressureElevationFeature,
        (100 - temperaturePredict) / 2
      ))

      val humidityPredict = this.humidityModel.predict(initData.humidityVector).toInt

      val adjustHumidityPredict = if(humidityPredict > 100) 100 else humidityPredict

      val weatherConditionPredict = this.weatherConditionModel.predict(Vectors.dense(
        temperaturePredict,
        airPressurePredict,
        adjustHumidityPredict
      )).toInt


      val newDateTimeString = addRandomTimeOnDate(initData.datetimeString)

      val weatherDataPredict = new WeatherData(newDateTimeString,
        initData.latitude,initData.longitude,initData.locationLabel,
        weatherConditionPredict,temperaturePredict,airPressurePredict,adjustHumidityPredict)

      weatherStimulationFile.write("%s\n".format(weatherDataPredict.toString))
//      println(weatherDataPredict.toString())

      stimulation(weatherDataPredict,noOfStimulation - 1)
    }
  }

}
