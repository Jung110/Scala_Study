### Week 1

```Scala
import scala.collection.mutable

trait Quiz[A,B]{
  def doit(n : B): A
}

class Quiz1() extends Quiz[(Int,Int),List[String]]{
  override def doit(n: List[String]): (Int,Int) = {
    (n.size,(for (v<-n) yield v.size).sum)
  }

}
class Quiz2() extends Quiz[Array[Int],Int]{

  override def doit(n: Int): Array[Int] = {
    if (n<1) return Array[Int]()
    else if(n==1) return Array(0)
    var anw = Array(0,1)
    for (num<-(2 until n)) {
      anw = anw :+ (anw.apply(num-2)+ anw.apply(num-1))
    }
    anw
  }

}



object Main {
  def main(args: Array[String]): Unit = {
    val q1 = new Quiz1()
    val q2 = new Quiz2()
    println(q1.doit(List("apple", "basket", "candy")))
    print(s"""
    ${q2.doit(-1).isEmpty}
    ${q2.doit(0).isEmpty}
    ${q2.doit(1).toSeq == List(0)}
    ${q2.doit(2).toSeq == List(0, 1)}
    ${q2.doit(5).toSeq == List(0, 1, 1, 2, 3)}
    ${q2.doit(10).toSeq == List(0, 1, 1, 2, 3, 5, 8, 13, 21, 34)}
    """)

    type Row = Array[Int]

    def Row(xs: Int*) = Array(xs: _*)

    type Matrix = Array[Row]

    def Matrix(xs: Row*) = Array(xs: _*)

    implicit class RowOperators(_this: Row) {
      def *(other: Array[Int]): Int = {
        val temp = for (num <- (0 until other.size)) yield {
          _this.apply(num) * other.apply(num)
        }
        temp.sum
      }
    }

    implicit class MatrixOperators(_this: Matrix) {
      def *(other: Matrix): Array[Array[Int]] = {
        val new_mat = for (row <- _this) yield {
          var new_row = Array[Int]()
          for (n <- other.indices) {
            val col = for (idx <- other.apply(0).indices) yield other.apply(idx).apply(n)
            new_row = new_row :+ (col.toArray * row)
          }
          println(new_row.toSeq)
          new_row
        }
        new_mat
      }
    }
    Matrix(Row(1, 2), Row(1, 2)) * Matrix(Row(1, 1), Row(1, 1))

  }
}
```

주소값 출력 해결

foreach 2번 사용하니 안에 정상값이 들어 있는 것을 확인 하여 무시하고 코딩하니 잘 작동하였습니다.



homework3

```scala

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

```
