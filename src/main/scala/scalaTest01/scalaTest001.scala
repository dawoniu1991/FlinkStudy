package scalaTest01

/**
 * @author jiangfan
 * @date 2022/8/16 19:05
 */
import javatest01.Student

import java.text.SimpleDateFormat
import java.util.Date

object scalaTest001 {
  def main(args: Array[String]): Unit = {
    val strings = "aaa,bbb,qqq".split(",")
    println(strings.apply(0))
    val str = strings.apply(1)
    println(str)
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val value = new Date()
    println(format.format(value))

    val qq = new Student("qq", "m", 11)
    println(qq.getName)
    fun(qq)
    println("=====")
    println(qq.getName)
  }

  def fun(input:Student): Unit ={
    input.setName("wwee")
  }

}
