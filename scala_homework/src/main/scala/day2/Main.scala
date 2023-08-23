package day2
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}




object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.driver.bindAddress", "127.0.0.1")
      .master("local[*]")
      .appName("Spark Home Work")
      .getOrCreate()
    //  set Path
    val dataPath = System.getenv("SPARK_DATA")
    val path = s"${dataPath}/2008_temp.csv"
    val airportPath = s"${dataPath}/airports.csv"
    val planeDataPath = s"${dataPath}/plane-data.csv"
    //  load Data
    val rdd = spark.sparkContext.textFile(path)
    rdd.take(5).foreach(println)
    val rddCsv = rdd.map(_.split(","))
    rddCsv.take(5).foreach(println)
    val airportRdd = spark.sparkContext.textFile(airportPath)
    airportRdd.take(5).foreach(println)
    val airportRddCsv = airportRdd.map(s=>{
      s.split(",")
    })
    airportRddCsv.take(5).foreach(println)

    val planeDataRdd = spark.sparkContext.textFile(planeDataPath)
    val planeDataRddCsv = planeDataRdd.map(f => {
      f.split(",")
    })


    val totalArrivals = rddCsv.count()
    var filteredRdd = rddCsv.filter(_(15) != "NA").filter(_(15).toInt > 0)


    val sumTimeForDelayedArrival = filteredRdd.map(
      {
        case a =>( a(17) , a(15).toInt)
      }
    ).reduceByKey(_+_)

    filteredRdd = rddCsv.filter(_(14) != "NA").filter(_(14).toInt > 0)

    val sumTimeForDelayedDest = filteredRdd.map(
      {
        case a => (a(17), a(14).toInt)
      }
    ).reduceByKey(_ + _)

    rddCsv.take(5).foreach(println)

  }


}
