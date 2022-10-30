package com.work

/**
 * @author jiangfan
 * @date 2022/7/1 16:15
 */
object MyUtil {
  def extract(line:String):(String, String, Double)={
    val arr = line.split(",")
    Tuple3( arr(0), arr(1), arr(2).toDouble )
  }
}
