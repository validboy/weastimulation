package com.weastimulation

import java.io._
import java.text.SimpleDateFormat
import java.util.SimpleTimeZone

/**
  * Created by validboy on 8/29/16.
  * The main Object to download observation data from BOM
  */
object GetTrainingData {

  def convertLocalTimeToUTC(ds: String): String = {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
    val date = dateFormat.parse(ds)
    dateFormat.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"))
    val dateAsString = dateFormat.format(date)
    dateAsString.replaceAll("\\+00","Z")

  }

  def downloadData(stationConfigList: String, trainingFileSavePath: String) = {

    val trainingFile = new PrintWriter(new File(trainingFileSavePath))
    val stationListFileStream : InputStream = getClass.getResourceAsStream("/" + stationConfigList)
    val stationListLines = scala.io.Source.fromInputStream(stationListFileStream).getLines.zipWithIndex
    for ((line,index) <- stationListLines){
      if (index != 0) {
        val cols = line.split("\\|",-1)
        var sid, stnName, latitube, longitube, postCode, timeZone= ""
        for ( i <- 0 to 5 ) {
          i match {
            case 0 => sid = cols(i)
            case 1 => stnName = cols(i)
            case 2 => latitube = cols(i)
            case 3 => longitube = cols(i)
            case 4 => postCode = cols(i)
            case 5 => timeZone = cols(i)
            case _ => null
          }
        }

        for( yearAndMonth <- List("201507","201508","201509","201510",
          "201511","201512","201601","201602","201603","201604","201605","201606")) {

          val bomURL = "http://www.bom.gov.au/climate/dwo/yyyymm/text/IDCJDWpostcode.yyyymm.csv"
          val weatherDataURL = bomURL.replaceAll("yyyymm", yearAndMonth).replaceAll("postcode",postCode)

          val weatherDataLines = scala.io.Source.fromURL(weatherDataURL,"ISO-8859-1").getLines.zipWithIndex
          var headerIndex = 0

          for ((weatherDataLine, weatherDataIndex) <- weatherDataLines ){

            //Get the line number prior to the real data
            if ( weatherDataLine.contains("3pm MSL pressure (hPa)") ) headerIndex = weatherDataIndex + 1

            if (weatherDataIndex >= headerIndex && headerIndex != 0) {

              val weatherDataCols = weatherDataLine.split(",",-1)

              val localDate = weatherDataCols(1)
              val minTempertureInDay =  weatherDataCols(2)
              val temperature9AM = weatherDataCols(10)
              val airPressure9AM = weatherDataCols(15)
              val humidity9AM = weatherDataCols(11)
              val rainfall = weatherDataCols(4)
              val temperature3PM = weatherDataCols(16)
              val airPressure3PM = weatherDataCols(21)
              val humidity3PM = weatherDataCols(17)

              if(localDate != "" && temperature9AM != "" && airPressure9AM != "" && humidity9AM != "" &&
                temperature3PM != "" && airPressure3PM != "" && humidity3PM != "" && minTempertureInDay != "" &&
                rainfall != ""){
                val formatLocalDate =
                  if(weatherDataCols(1).length < 10) weatherDataCols(1).substring(0,8) + "0" + weatherDataCols(1).substring(8,9)
                  else weatherDataCols(1)
                val convertUTCtime9AM = convertLocalTimeToUTC(formatLocalDate + "T09:00:00" + timeZone)
                val convertUTCtime3PM = convertLocalTimeToUTC(formatLocalDate + "T15:00:00" + timeZone)
                val weatherCondition =
                  if(rainfall.toDouble > 0 && minTempertureInDay.toDouble < 0) 2
                  else if(rainfall.toDouble > 0) 1
                  else 0
                trainingFile.write("%s|%s|%s|%s|%d|%s|%s|%s\n".format(stnName,latitube,longitube,convertUTCtime9AM,
                  weatherCondition,temperature9AM,airPressure9AM,humidity9AM))
                trainingFile.write("%s|%s|%s|%s|%d|%s|%s|%s\n".format(stnName,latitube,longitube,convertUTCtime3PM,
                  weatherCondition,temperature3PM,airPressure3PM,humidity3PM))


              }
            }
          }
        }
      }
    }
    stationListFileStream.close()
    trainingFile.close()
  }

  def main(args: Array[String]) {

    downloadData("StationList.txt","src/main/resources/observationData.txt")

  }


}
