package apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100L)

//    val streamFile: DataStream[String] = env.readTextFile("F:\\myflinkstudy\\flinktest\\src\\main\\resources\\sensor.txt")

    val streamFile: DataStream[String] = env.socketTextStream("localhost", 7777)

    val dataStream: DataStream[SensorReading] = streamFile.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)

    })
//      .assignAscendingTimestamps(_.timestamp*1000)
//      .assignTimestampsAndWatermarks(new  MyAssigner())
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp*1000
      })

    val value01: DataStream[(String, Double)] = dataStream.map(data => (data.id, data.temperature)).keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .reduce((x, y) => (x._1, x._2.min(y._2)))

    value01.print("min temp")
    dataStream.print("input data")
    env.execute("window test")

  }
}


class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading]{
  val bound:Long=60*1000
  var maxTs:Long=Long.MinValue
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs-bound)
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    maxTs=maxTs.max(t.timestamp*1000)
    t.timestamp*1000
  }
}



class MyProcess() extends KeyedProcessFunction[String,SensorReading,String]{
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {

//    ctx.timerService().
  }
}
