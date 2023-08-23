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
// match
    if (n < 1) return Array[Int]()
    else if (n == 1) return Array(0)
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


  }
}