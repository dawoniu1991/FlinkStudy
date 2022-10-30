package mytest

import javatest01.JavaTest003

import java.util.StringJoiner
import scala.collection.mutable

/**
 * @author jiangfan
 * @date 2022/9/7 17:23
 */
object test08 {
  def main(args: Array[String]): Unit = {


    val strings = "aa,bb,cc".split(",")
    println(strings)
    val qwe="aa,bb,cc".split(",")
    println(qwe)
    val joiner = new StringJoiner(",")
    val test00 = new JavaTest003()
    val ww: mutable.HashMap[String, String] = new mutable.HashMap[String,String]
    ww.put("aa","111")
    test00.data=ww
    println(test00)
    val qq=test00
    println(qq)
    val name="qqww"
    val address="beijing"
    val country="cn"
//    println(joiner.add(name))
//    println(joiner.add(name).add(address))
    println(joiner.add(name).add(address).add(country))

  }

}
