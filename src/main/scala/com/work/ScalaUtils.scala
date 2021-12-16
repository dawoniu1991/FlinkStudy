package com.work

import java.util.Properties

/**
 * @author jiangfan
 * @date 2021/12/8 13:51
 */
object ScalaUtils {
  def loadConfig(): Properties = {
    val p = new Properties()
    val in = this.getClass.getClassLoader.getResourceAsStream("config.properties")
    p.load(in)
    in.close()
    p
  }
}
