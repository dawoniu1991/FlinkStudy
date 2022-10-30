package mytest

/**
 * @author jiangfan
 * @date 2022/8/13 18:02
 */
import scala.util.Random
import java.util.concurrent.TimeUnit

object SynchronizedDemo {

//   @volatile   var inc: Int = 0
   var inc: Int = 0

//  def addOne(): Unit = this.synchronized {
      def addOne(): Unit = AnyRef.synchronized({
    TimeUnit.SECONDS.sleep(1)
    inc += 1
//  }
  })

//  def addOne(): Unit = {
//    TimeUnit.SECONDS.sleep(1)
//    inc  += 1
//  }

  def main(args: Array[String]): Unit = {
    for (i <- 1 to 10) {
      new Thread {
        override def run(): Unit = {
          println(s"run thread with object method $i,$inc")
          addOne()
        }
      }.start()
    }
    println("qwe".substring(1))
    val value = new Random()
    println(value.nextInt(88)+100)
    TimeUnit.SECONDS.sleep(15)
    println(inc)
//    val instance = new SynchronizedDemo
//    for (i <- 1 to 10) {
//      new Thread {
//        override def run(): Unit = {
//          println(s"run thread with class method $i")
//          instance.addOne()
//        }
//      }.start()
//    }
//    while (true) {
//      println(s"object inc=$inc, class inc=${instance.inc}")
//      TimeUnit.SECONDS.sleep(1)
//    }
  }


}

//class SynchronizedDemo {
//  private var inc: Int = 0
//
//  def addOne(): Unit = this.synchronized {
//    TimeUnit.SECONDS.sleep(1)
//    inc  = 1
//  }
//}