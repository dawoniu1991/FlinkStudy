package sinktest

import java.sql.{Connection, PreparedStatement}

import apitest.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.watermark.Watermark

object JdbcSinkTest {
  def main(args: Array[String]): Unit = {

  }
}


class MyJdbcSink() extends RichSinkFunction[SensorReading]{
  var conn:Connection=_
  var insertStmt:PreparedStatement=_
  var updateStmt:PreparedStatement=_

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = super.invoke(value, context)

  override def close(): Unit = super.close()
}


class MyTestPeriodicWatermark extends AssignerWithPeriodicWatermarks[SensorReading]{
  val bound:Long=60*1000
  var maxTs:Long=Long.MinValue
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs-bound)
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    maxTs=maxTs.max(t.timestamp)
    t.timestamp
  }
}

class MyTestPunctuatedWatermark  extends AssignerWithPunctuatedWatermarks[SensorReading]{
  val bound:Long=60*1000
  override def checkAndGetNextWatermark(t: SensorReading, l: Long): Watermark = {
    if(t.id=="sensor_1"){
      new Watermark(l-bound)
    }else{
      null
    }
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    t.timestamp
  }
}