package com.weastimulation

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * Created by validboy on 8/30/16.
  * Test the attributes and derived
  * features in Weather Data
  */
@RunWith(classOf[JUnitRunner])
class WeatherDataTest extends FunSuite {

  test("Test 2: Weather Data abstract class Unit Test") {

    val syd = new WeatherData("2015-06-03T23:00:00Z",-33.86,151.21,"sydney",2,22.2,1011.1,56)

    assert(syd.elevation === 34)
    assert(syd.month === 6)
    assert(syd.dayInMonth === 3)
    assert(syd.isNorthHemisphere === false)
    assert(syd.locationLabel === "sydney")
    assert(syd.weatherConditionLabel === 2)
    assert(syd.temperatureLabel === 22.2)
    assert(syd.airPressureLabel === 1011.1)
    assert(syd.humidityLabel === 56)

  }
}
