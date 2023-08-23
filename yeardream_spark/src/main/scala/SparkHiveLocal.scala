package org.yeardream.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkHiveLocal extends App {
  val sparkHome = System.getenv("SPARK_HOME")
  val logDir = s"file:${sparkHome}/event"
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("SparkCreateTableExample")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", logDir)
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  // Create DataFrame
  val sampleDF = Seq((1, "James", 30, "M"),
    (2, "Ann", 40, "F"), (3, "Jeff", 41, "M"),
    (4, "Jennifer", 20, "F")
  ).toDF("id", "name", "age", "gender")

  // Create Hive Internal table
  sampleDF.write.mode(SaveMode.Overwrite)
    .saveAsTable("employee")

}