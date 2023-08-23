package org.yeardream.spark
// 전체를 JAR파일로 만들어서 전체를 스파크에 올려놓는다.
import org.apache.spark.sql.SparkSession

object Main extends App {
//  스파크 경로
  val sparkHome = System.getenv("SPARK_HOME")
  val logDir = s"file:${sparkHome}/event"
//  세션 생성
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkExample") // 앱이름을 지정하여 GUI에서 구분하기 위해서
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", logDir)
    .getOrCreate();

  println("First SparkContext:")
  println("APP Name :" + spark.sparkContext.appName);
  println("Deploy Mode :" + spark.sparkContext.deployMode);
  println("Master :" + spark.sparkContext.master);
  val df = spark.createDataFrame(
    List(("Scala", 25000), ("Spark", 35000), ("PHP", 21000)))
//  디버깅용 실제 사용 할떄는 지우고 실행
  df.show()
  df.count()

  val sparkSession2 = SparkSession.builder()
    .master("local[1]")
    .appName("SparkExample-2")
    .config("spark.eventLog.enabled", true)
    .config("spark.eventLog.dir", logDir)
    .getOrCreate();

  println("Second SparkContext:")
  println("APP Name :" + sparkSession2.sparkContext.appName);
  println("Deploy Mode :" + sparkSession2.sparkContext.deployMode);
  println("Master :" + sparkSession2.sparkContext.master);
}
