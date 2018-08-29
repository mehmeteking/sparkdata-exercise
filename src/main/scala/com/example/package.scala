package com

import org.apache.spark.SparkConf

package object example {
  type DataFrame = org.apache.spark.sql.DataFrame
  type Dataset[A] = org.apache.spark.sql.Dataset[A]

  type RDD[A] = org.apache.spark.rdd.RDD[A]
  val RDD = org.apache.spark.rdd.RDD
  type Row = org.apache.spark.sql.Row
  val Row = org.apache.spark.sql.Row
  type Session = org.apache.spark.sql.SparkSession
  val Session = org.apache.spark.sql.SparkSession
  type Context = org.apache.spark.SparkContext
  val Context = org.apache.spark.SparkContext

  type Struct = org.apache.spark.sql.types.StructType
  val Struct = org.apache.spark.sql.types.StructType
  type Field = org.apache.spark.sql.types.StructField
  val Field = org.apache.spark.sql.types.StructField

  val StringType = org.apache.spark.sql.types.StringType
  val IntegerType = org.apache.spark.sql.types.IntegerType
  val DoubleType = org.apache.spark.sql.types.DoubleType
  val BooleanType = org.apache.spark.sql.types.BooleanType

  val defaultDir = "file:///${system:user.dir}/data"
  val dataDirKey = "spark.sql.warehouse.dir"
  val defaultMaster = "local"

  def newConf(appName: String, dataDir: String = defaultDir, master: String = defaultMaster): SparkConf =
    new SparkConf().setAppName(appName).setMaster(master).set(dataDirKey, dataDir)

  def newSession(conf: SparkConf = newConf("example")): Session =
    Session.builder().config(conf).getOrCreate()

  val printPartition: Iterator[Any] => Unit = it => println(s"[${it.mkString(",")}]")

  trait SparkProps {
    def session: Session
    val spark: Context = session.sparkContext
  }
  trait SparkApp extends App with SparkProps {
    val session = newSession()
  }
}
