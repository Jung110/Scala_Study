
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics


object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.driver.bindAddress", "127.0.0.1")
      .master("local[*]")
      .appName("Spark Home Work")
      .getOrCreate()
    //  set Path
    val dataPath = "D:\\git\\data"
    val path = s"${dataPath}/2008_temp.csv"
    val airportPath = s"${dataPath}/airport.csv"
    val planeDataPath = s"${dataPath}/plane.csv"
    //  load Data
    val rdd = spark.sparkContext.textFile(path)
    val rddCsv = rdd.map(_.split(","))
    val airportRdd = spark.sparkContext.textFile(airportPath)
    val airportRddCsv = airportRdd.map(s=>{
      s.split(",").map(f=>f.toString)
    })
    val planeDataRdd = spark.sparkContext.textFile(planeDataPath)
    val planeDataRddCsv = planeDataRdd.map(f => {
      f.split(",")
    })


    val totalArrivals = rddCsv.count()
    var filteredRdd = rddCsv.filter(_(14) != "NA").filter(_(14).toInt > 0)


    val sumTimeForDelayedArrival = filteredRdd.map(
      {
        case a =>( a(16) , a(14).toInt)
      }
    ).reduceByKey(_+_)

    filteredRdd = rddCsv.filter(_(14) != "NA")

    val countDelayedArrival = filteredRdd.map(
      {
        case a => (a(16),1)
      }
    ).reduceByKey(_ + _)

    val avgDelayedArri = sumTimeForDelayedArrival.join(countDelayedArrival).map(
      {
        case (key , (sum,count)) =>  (key, sum/count)
      }
    )

    filteredRdd = rddCsv.filter(_(15) != "NA").filter(_(15).toInt > 0)

    val sumTimeForDelayedDest = filteredRdd.map(
      {
        case a => (a(17), a(15).toInt)
      }
    ).reduceByKey(_ + _)
    //    큰타옴표 제거

    val aiportName = airportRddCsv.map(
      {
        case a => (a(0).stripPrefix("\"").stripSuffix("\""), a(1).toString)
      }
    )

    val joinedByArri  = avgDelayedArri.join(aiportName).map(
      {
        case  a => (a._2._2 ,a._2._1 )
      }
    )
    joinedByArri.foreach(println)

    //    시간관계상 바로 3번
    filteredRdd = rddCsv.filter(_(14) != "NA")
    val sumTimeForDelayedPlane = filteredRdd.map(
      {
        case a => (a(10), if (a(14).toInt <0) 0 else a(14).toInt)
      }
    ).reduceByKey(_ + _)
    val countDelayedplane = filteredRdd.map(
      {
        case a => (a(10), 1)
      }
    ).reduceByKey(_ + _)
    val avgDelayedTimePlane = sumTimeForDelayedPlane.join(countDelayedplane).map(
      {
        case (key, (sum, count)) => (key, sum / count)
      }
    )
    val filteredPlaneRdd = planeDataRddCsv.filter(_.size>1).filter(_(8) != "None")
    val tailNumYear = filteredPlaneRdd.map(
      {
        case a => (a(0).stripPrefix("\"").stripSuffix("\""), (2008 - a(8).toInt))
      }
    )
    val joinedPlaneData = avgDelayedTimePlane.join(tailNumYear)

    val delayedTime = joinedPlaneData.map(
      {
        case a=>(a._2._1.toDouble)
      }
    )
    val oldPlane = joinedPlaneData.map(
      {
        case a => (a._2._2.toDouble)
      }
    )
    val corr= Statistics.corr(delayedTime, oldPlane, "pearson")
    println("-"*100+"\n"*5+s"corr:${corr}")
  }


}
