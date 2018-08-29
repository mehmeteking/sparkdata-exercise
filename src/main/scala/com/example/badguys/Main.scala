package com.example
package badguys

object Main extends SparkApp {

  val classLoader = getClass.getClassLoader
  def getPath(fileName: String) =
    classLoader.getResource(s"data/$fileName").getPath
  def readCsv(fileName: String): DataFrame =
    session.read.format("csv").option("header", "true").load(getPath(fileName))

  val businesses: DataFrame = readCsv("businesses.csv")
  val inspections: DataFrame = readCsv("inspections.csv")
  val violations: DataFrame = readCsv("violations.csv")

  val inspector = new Inspector(session)
  val badGuys: Dataset[Inspector.Result] = inspector.investigate(businesses, inspections, violations)

  businesses.show
  spark.stop
}
