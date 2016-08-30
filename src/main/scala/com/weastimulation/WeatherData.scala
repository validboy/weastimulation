package com.weastimulation


import java.awt.image.BufferedImage
import java.awt.Color
import javax.imageio.ImageIO
import org.apache.spark.mllib.linalg.Vectors
import Math.abs
/**
  * Created by validboy on 8/28/16.
  * Class to describe Weather data
  */
class WeatherData(
                 val datetimeString: String,
                 val latitude: Double,
                 val longitude: Double,
                 val locationLabel: String = "",
                 val weatherCondition: Int = 0,
                 val temperature: Double = 0.0,
                 val airPressure: Double = 0.0,
                 val humidity: Int = 0
                 ) {

  /*
   * Get elevation from elevation map
   */
  val elevationBufImg: BufferedImage = ImageIO.read(new java.io.File("src/main/resources/elevation.bmp"))
  val XCoordinateFromLongitude = (((longitude + 180) / 360) * 1080).toInt
  val YCoordinateFromLatitude = (((90 - latitude ) / 180) * 540).toInt

  val red = new Color(elevationBufImg.getRGB(XCoordinateFromLongitude, YCoordinateFromLatitude)).getRed
  /*
   * The highest point on Earth is 8848, so simply use 8848/255 = 34 multiple
   * red degree to get the estimated elevation
   */
  val elevation = red * 34


  /*
   * Derived the features from the weather observation
   *
   */

  /* e.g North Hemisphere
   * Month between
   * 12 1 2 is Winter
   * 3 4 5 is Spring
   * 9 10 11 is Autumn
   * 6 7 8 is Summer
   */
  val month = datetimeString.substring(5,7).toInt
  val dayInMonth = datetimeString.substring(8,10).toInt
  val hour = datetimeString.substring(11,13).toInt
  val dayNightFeature = abs(hour - 12) - 12 - 6
  val isNorthHemisphere = latitude > 0
  val seasonFeature = dayInMonth + dayNightFeature +
    month match {
      case 12 => if(isNorthHemisphere) 30 else 270
      case 1 => if(isNorthHemisphere) 0 else 330
      case 2 => if(isNorthHemisphere) 60 else 300
      case 3 => if(isNorthHemisphere) 90 else 240
      case 4 => if(isNorthHemisphere) 120 else 210
      case 5 => if(isNorthHemisphere) 150 else 180
      case 9 => if(isNorthHemisphere) 240 else 90
      case 10 => if(isNorthHemisphere) 210 else 120
      case 11 => if(isNorthHemisphere) 180 else 150
      case 6 => if(isNorthHemisphere) 270 else 30
      case 7 => if(isNorthHemisphere) 330 else 0
      case 8 => if(isNorthHemisphere) 300 else 60
      case _ => if(isNorthHemisphere) 0 else 0
    }


  /*
   * Get the oceanology feature Warm current/Cold current/Inland
   * From elevation map
   * To keep it simple, for most of the areas in the continent
   * the east coast of the continent has cold current
   * and the west coast of the continent has warm current
   * others we assume it's inland area
   *
   * NorthEast North NorthWest
   * East      *     West
   * SouthEast South SouthWest
   *
   * if any of the NorthEast/East/SouthEast then it'll affect by cold current
   * if any of the NorthWest/West/SouthWest then it'll affect by warm current
   *
   *
   * cold current -1
   * warm current 1
   * inland 0
   */
  val estimatedMarginX = 10
  val estimatedMarginY = 5

  val westMarginCoordinate =
    if(XCoordinateFromLongitude - estimatedMarginX < 0 ) 1080 - (XCoordinateFromLongitude - estimatedMarginX)
    else XCoordinateFromLongitude - estimatedMarginX

  val eastMarginCoordinate =
    if(XCoordinateFromLongitude + estimatedMarginX > 1080 ) estimatedMarginX - (1080 - XCoordinateFromLongitude)
    else XCoordinateFromLongitude + estimatedMarginX

  val northMarginCoordinate =
    if(YCoordinateFromLatitude - estimatedMarginY < 0 ) 540 - (YCoordinateFromLatitude - estimatedMarginY)
    else YCoordinateFromLatitude - estimatedMarginY

  val southMarginCoordinate =
    if(YCoordinateFromLatitude + estimatedMarginY > 540 ) estimatedMarginY - (540 - YCoordinateFromLatitude)
    else YCoordinateFromLatitude + estimatedMarginY

  val NorthEastRed = new Color(elevationBufImg.getRGB(eastMarginCoordinate, northMarginCoordinate)).getRed
  val EastRed = new Color(elevationBufImg.getRGB(eastMarginCoordinate, YCoordinateFromLatitude)).getRed
  val SouthEastRed = new Color(elevationBufImg.getRGB(eastMarginCoordinate, southMarginCoordinate)).getRed

  val NorthWestRed = new Color(elevationBufImg.getRGB(westMarginCoordinate, northMarginCoordinate)).getRed
  val WestRed = new Color(elevationBufImg.getRGB(westMarginCoordinate, YCoordinateFromLatitude)).getRed
  val SouthWestRed = new Color(elevationBufImg.getRGB(westMarginCoordinate, southMarginCoordinate)).getRed

  val oceanologyFeature =
    if(NorthEastRed == 0 || EastRed == 0 || SouthEastRed == 0) -1
    else if(NorthWestRed == 0 || WestRed == 0 || SouthWestRed == 0) 1
    else 0

  /*
   * Remap elevation and latitude to proper range
   * to be used on the regression model
   */
  val elevationFeature = (8868 - elevation)/ 8000
  val latitudeFeature = (0 - abs(latitude)) + 37.8

  /*
   * prediction or observation for Temperature/Pressure/Humidity
   */
  val temperatureFeature = temperature
  val airPressureFeature = airPressure
  val humidityFeature = humidity

  /*
   * Define the Labels for Temperature/Pressure/Humidity/Weather Condition
   */
  val temperatureLabel = temperature
  val airPressureLabel = airPressure
  val humidityLabel = humidity
  val weatherConditionLabel = weatherCondition

  /*
   * Construct Temperature feature Vector by
   * season/daynight/latitude/elevation/oceanology features
   */
  val temperatureVector = Vectors.dense(
      seasonFeature,
      dayNightFeature * 5,
      latitudeFeature * 17,
      elevationFeature,
      oceanologyFeature
  )

  /*
   * Construct the Pressure Vector by
   * latitude/elevation/temperature
   */
  val airPressureLatitudeFeature = (90 - abs(latitude)) / 2
  val airPressureElevationFeature = (8868 - elevation)/ 8000
  val airPressureTemperatureFeature = (100 - temperature) / 2

  val airPressureVector = Vectors.dense(
    airPressureLatitudeFeature,
    airPressureElevationFeature,
    airPressureTemperatureFeature
  )

  /*
   * Construct the Humidity Vector by
   * latitude/daynight/oceanology features
   */
  val humidityVector = Vectors.dense(
    latitudeFeature,
    dayNightFeature * 8,
    oceanologyFeature
  )

  /*
   * Construct the Weather Condition feature by
   * Temperature/Pressue/Humidity features
   */
  val weatherConditionVector = Vectors.dense(
    temperatureFeature,
    airPressureFeature,
    humidityFeature
  )

  /*
   * Defined the weather condition given by the mapping
   * 0 -> Suny
   * 1 -> Rain
   * 2 -> Snow
   */
  val weatherConditionToString = weatherCondition match{
    case 0 => "Sunny"
    case 1 => "Rain"
    case 2 => "Snow"
  }

  override def toString():String = "%s|%s,%s,%s|%s|%s|%+.2f|%.2f|%d".format(
    locationLabel,latitude,longitude,elevation,datetimeString,weatherConditionToString,
    temperature,airPressure,humidity)

}
