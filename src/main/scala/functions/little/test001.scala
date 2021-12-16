package functions.little

import scala.util.Random

/**
 * @author jiangfan
 * @date 2021/4/25 14:37
 */
object test001 {
  def main(args: Array[String]): Unit = {
    val rand = new Random()
    val i = rand.nextInt(10)
    println(i)
    import java.text.SimpleDateFormat
    val dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateStr = dateformat.format(System.currentTimeMillis)
    println(dateStr)
    val aa: Double =33.toDouble
    val bb: Double =22.toDouble
    println((aa + bb) / 2)
  }
}
