package scalaTest01

import java.sql.{Connection, DriverManager}

/**
 * @author jiangfan
 * @date 2022/9/29 19:38
 */
object ScalaTest005 {
  def main(args: Array[String]): Unit = {
    println("aaaa")
    println(scalaTest004.name)
    println(scalaTest004.name)
//    println(scalaTest004.getName())
//    println(scalaTest004.getName())
//    println(scalaTest004.getName())

    var seq:Seq[Int] = Seq(52, 80, 10, 8 )
    seq.foreach((element:Int) => print(element+" "))
    println("\nAccessing element by using index")
    println(seq(2))
    println(seq.sum)
  }
}
