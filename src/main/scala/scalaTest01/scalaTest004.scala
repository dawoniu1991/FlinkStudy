package scalaTest01

/**
 * @author jiangfan
 * @date 2022/8/16 19:05
 */
import javatest01.Student

import java.text.SimpleDateFormat
import java.util.Date

object scalaTest004 {
  var name=init()
  def init(): String = {
    println("init begin")
    val strings = "aaa,bbb,qqq"
    strings
  }

  def getName(): String ={
    name+"--qqww"
  }

}
