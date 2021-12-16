package MyScalaTest

import functions.ImpressionFilter
import org.scalatest.FunSuite

import scala.io.Source


class test01 extends FunSuite  {
//  def main(args: Array[String]): Unit = {
//    println("aaa")
//  }

  test("m-impression") {
    println("begin~~~~~~~~~~~")

    val impressfilter = new ImpressionFilter
    //    val file=Source.fromFile("sample/adn-tracking-v3-user-action-impression-log-txt-0.log-new-1")
    //    val file=Source.fromFile("sample/adn-tracking_v3_fluentd_install-log_txt-0.log")
    //    val file=Source.fromFile("sample/adn-tracking_v3_fluentd_impression-log_txt-0.log")
    val file=Source.fromFile("sample/test01.txt")
    val lines = file.getLines
    var counter = 0
    var total = 0
    var fiter = 0
    for(line <- lines)
    {
      println(line)
      total += 1
      if (impressfilter.filter((line,line))) {
        //        if (parseLine._2.platform.equals("ios")){
        //          if (StringUtils.isNotBlank(idfv) && idfv.length>10){
        counter += 1
        println("success.......")

      } else {
        fiter+=1
                println("filtered...")
      }
    }
    println("total = " + total)
    println("filtered nums =" + fiter)
    println("true nums = " + counter)
    file.close
  }

}
