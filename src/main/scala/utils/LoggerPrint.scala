package utils

import org.slf4j.{Logger, LoggerFactory}

/**
 * @author jiangfan
 * @date 2022/10/10 16:45
 */
object LoggerPrint {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  def info(x:Any,logLevel:Int)={
    if(logLevel==0){
      logger.info(x.toString)
    }else if(logLevel==1){
      println(x.toString)
    }
  }

  def info(x:Any)={
      logger.info(x.toString)
  }
}
