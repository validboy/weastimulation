package com.weastimulation

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * Created by validboy on 8/30/16.
  * Test on the observation data layout
  */
@RunWith(classOf[JUnitRunner])
class GetTrainningDataUnitTest extends FunSuite {

  test("Unit test 1: GetTrainingData file layout testing") {

    val sc = new SparkContext(new SparkConf().setAppName("Weather Model Stimulation Test").setMaster("local[*]"))

    val obsDataLines = sc.textFile("src/main/resources/observationData.txt")

    val obsDataLine = obsDataLines map {line => line.split("\\|", -1) }

    val correctLine = obsDataLine map {line => line.lengthCompare(8) }

    val linesInFile = obsDataLines.count()
    val correctLinesInFile = correctLine.count()
    assert(linesInFile === correctLinesInFile)
    sc.stop

  }

}
