package utils

import java.util.Properties

/**
 * @author jiangfan
 * @date 2022/10/10 16:39
 */
object ScalaUtil {
    def loadConfig():Properties={
      val p=new Properties()
      val in=this.getClass.getClassLoader.getResourceAsStream("config.properties")
      p.load(in)
      in.close()
      p
    }
}
