
In this Scala project, it'll demostrate how to use Spark MLlib to create simple models and stimulate the weather changes.

# Workflows
 
The weather observation data is retrieved from BOM(www.bom.gov.au). 
An automatic download process has been implemented, also proper cleansing and transformation will apply on the raw data and then conform into predefined layout as below.
Location|Latitude|Longitude|Elevation|Weather condition|Temperature|Pressure|Humidity
Since there is no snow observation in the data, but we could assume if the minimum temperature < 1 and it rain then it will snow.
 
Four models has been built for predicting the Temperature, Pressure, Humidity and Weather condition.
Before start to create model, let’s do some analysis and good guesses on the features for each of them.

Temperature, Pressure and Humidity models are linear regression model. To keep the solution simple, 
the main idea is to transfrom/remap the features from non-linear to linear then fit into our model. The features using here are Season/Geography/Oceanlogy/DayNightTime/Topography etc.

Weather condition would use Classification model(Decision tree) to predict the weather changes based on the features like Temperature/Pressure/Humidity etc.

A proper Model testing has been covered, and currently the prebuild model has a very good accurary to the weather prediction.

# Usage
1. sbt "run-main com.weastimulation.StimulateWeatherData"
To generate the weaterh stimulation by prebuild mode or adhoc mode.
2. sbt "run-main com.weastimulation.GetTrainingData"
Automatically download weather data from BOM by seting the configure file on resource files.
3. sbt "run-main com.weastimulation.WeatherModel"
Rebuild the model by new tranning data

