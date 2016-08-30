package com.weastimulation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by validboy on 8/30/16.
  * Main class to start the weather prediction
  */
object StimulateWeatherData {

  def main(args: Array[String]) {

    def exitWithErr(message: String) = {

      println(message)
      sc.stop()
      System.exit(1)

    }

    lazy val sc = new SparkContext(new SparkConf().setAppName("Weather Model Stimulation").setMaster("local"))
    lazy val stimulator = new WeatherStimulation(sc)

    println("Auto mode or Manual Mode? Type A or M ")
    val mode = readLine("prompt> ").toUpperCase

    if(mode != "A" && mode != "M"){
      exitWithErr("ERROR: invalid mode type!")
    }


    if(mode == "M"){
      println("Enter Start Date YYYY-MM-DDTHH:MI:SSZ e.g. 2015-06-03T23:00:00Z")
      val startDate = readLine("prompt> ")
      if(!startDate.matches("[0-9]{4}\\-[0-1][0-9]\\-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]Z")){
        exitWithErr("ERROR: invalid Date format!")
      }

      println("Enter Latitude: e.g. -33.86")
      val latitude = readLine("prompt> ")
      if(!latitude.matches("-?[0-9]+\\.?[0-9]*")){
        exitWithErr("ERROR: invalid Latitude number!")
      }

      println("Enter Longitude: e.g. 151.21")
      val Longitude = readLine("prompt> ")
      if(!Longitude.matches("-?[0-9]+\\.?[0-9]*")){
        exitWithErr("ERROR: invalid Longitude number!")
      }

      println("Enter Stimulation Times: e.g. 10")
      val stimulationTimes = readLine("prompt> ")
      if(!stimulationTimes.matches("[0-9]+")){
        exitWithErr("ERROR: invalid Stimulation number!")
      }

      println("Enter Place Label [Optional] : e.g. Sydney")
      val placeLabel = readLine("prompt> ")

      val adHoc = new WeatherData(startDate,latitude.toDouble,Longitude.toDouble,placeLabel)

      stimulator.stimulation(adHoc,stimulationTimes.toInt)

    }
    else{

      println("Enter Stimulation Times: e.g. 10")
      val stimulationTimes = readLine("prompt> ")
      if(!stimulationTimes.matches("[0-9]+")){
        exitWithErr("ERROR: invalid Stimulation number!")
      }

      val syd = new WeatherData("2015-06-03T23:00:00Z",-33.86,151.21,"Sydney")
      val mel = new WeatherData("2015-07-12T23:00:00Z",-37.83,144.98,"Melbourne")
      val adl = new WeatherData("2015-06-03T23:00:00Z",-34.92,138.62,"Adelaide")
      val per = new WeatherData("2015-06-14T23:00:00Z",-31.95,115.86,"Perth")
      val dar = new WeatherData("2015-08-03T23:00:00Z",-12.46,130.84,"Darwin")
      val cai = new WeatherData("2015-06-11T23:00:00Z",-16.92,145.78,"Cairns")
      val bri = new WeatherData("2015-10-03T23:00:00Z",-27.47,153.03,"Brisbane")
      val asp = new WeatherData("2015-06-16T23:00:00Z",-23.70,133.88,"Alice Springs")
      val der = new WeatherData("2015-06-03T23:00:00Z",-17.39,123.68,"Derby")
      val kar = new WeatherData("2015-11-03T23:00:00Z",-20.73,116.86,"Karratha")
      val can = new WeatherData("2016-01-03T23:00:00Z",-35.30,149.13,"Canberra")
      val hob = new WeatherData("2015-06-03T23:00:00Z",-42.88,147.13,"Hobart")
      val ufp = new WeatherData("2015-09-03T23:00:00Z",-66.6,151.21,"Unknown Frozen places")

      stimulator.stimulation(syd,stimulationTimes.toInt)
      stimulator.stimulation(mel,stimulationTimes.toInt)
      stimulator.stimulation(adl,stimulationTimes.toInt)
      stimulator.stimulation(per,stimulationTimes.toInt)
      stimulator.stimulation(dar,stimulationTimes.toInt)
      stimulator.stimulation(cai,stimulationTimes.toInt)
      stimulator.stimulation(bri,stimulationTimes.toInt)
      stimulator.stimulation(asp,stimulationTimes.toInt)
      stimulator.stimulation(der,stimulationTimes.toInt)
      stimulator.stimulation(kar,stimulationTimes.toInt)
      stimulator.stimulation(can,stimulationTimes.toInt)
      stimulator.stimulation(hob,stimulationTimes.toInt)
      stimulator.stimulation(ufp,stimulationTimes.toInt)


    }

    stimulator.closeFileStream()
    sc.stop()
    println("src/main/resources/weatherStimulation.txt is generated!")

  }



}
