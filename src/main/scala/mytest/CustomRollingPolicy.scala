package mytest

import org.apache.flink.streaming.api.functions.sink.filesystem.{PartFileInfo, RollingPolicy}
import org.joda.time.{DateTime, DateTimeZone}

/**
 * @author jiangfan
 * @date 2021/3/29 19:39
 */
class CustomRollingPolicy  extends RollingPolicy [String,String] {
  override def shouldRollOnCheckpoint(partFileState: PartFileInfo[String]): Boolean = false

  override def shouldRollOnEvent(partFileState: PartFileInfo[String], element: String): Boolean = false

  override def shouldRollOnProcessingTime(partFileState: PartFileInfo[String], currentTime: Long): Boolean ={
    val a = new DateTime().withZone(DateTimeZone.forID("+08:00")).withMillis(currentTime)
    a.getMinuteOfHour % 3 ==0
  }

}
