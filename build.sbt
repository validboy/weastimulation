organization := "com.weastimulation"
version := "0.1.0"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  "org.apache.spark" % "spark-mllib_2.11" % "2.0.0",
  "junit" % "junit" % "4.10" % "test",
  "org.scalatest" % "scalatest_2.11" % "2.1.3" % "test" 
)

mainClass in assembly := Some("com.weastimulation.StimulateWeatherData")

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    MergeStrategy.discard
}

