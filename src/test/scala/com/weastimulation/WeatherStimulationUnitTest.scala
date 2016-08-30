package com.weastimulation

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * Created by validboy on 8/30/16.
  * Weather stimulation Unit test
  */

@RunWith(classOf[JUnitRunner])
class WeatherStimulationUnitTest extends FunSuite {

  test("Unit test 4: Weather Stimulation File Generation Test") {

    val sc = new SparkContext(new SparkConf().setAppName("WeatherStimulation File Generation Test").setMaster("local"))
    val syd = new WeatherData("2015-06-03T23:00:00Z",-33.86,151.21,"sydney")

    val testFilePath = "src/test/resources/weatherStimulation_test.txt"
    val stimulator = new WeatherStimulation(sc,"src/test/resources/weatherStimulation_test.txt")

    stimulator.stimulation(syd,100)

    val fileCount = sc.textFile(testFilePath)

    stimulator.closeFileStream()

    assert(fileCount.count == 100)



    ////
    sc.stop()


  }

}
