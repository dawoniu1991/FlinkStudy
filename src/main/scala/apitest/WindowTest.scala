package apitest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import sourcetest.SensorReading

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

//    val value01: DataStream[(String, Double)] = dataStream.map(data => (data.id, data.temperature)).keyBy(_._1)
//      .timeWindow(Time.seconds(10))
//      .reduce((x, y) => (x._1, x._2.min(y._2)))

    // 开窗聚合操作
    val value01 = dataStream
      .keyBy(_.id)
      // 窗口分配器
      .timeWindow(Time.seconds(10))    // 10秒大小的滚动窗口
      //        .window( EventTimeSessionWindows.withGap(Time.seconds(1)) )    // 会话窗口
      //        .window( TumblingEventTimeWindows.of(Time.hours(1), Time.minutes(10)) )     // 带10分钟偏移量的1小时滚动窗口
      //        .window( SlidingProcessingTimeWindows.of(Time.hours(1), Time.minutes(10)) )     // 1小时窗口，10分钟滑动一次
      //        .countWindow( 10, 2 )    // 滑动计数窗口

      // 可选API
      //        .trigger()
      //        .evictor()
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[SensorReading]("late-data"))

      // 窗口函数
      //      .minBy("temperature")
      //      .reduce( (curState, newData) => SensorReading(newData.id, newData.timestamp + 1, curState.temperature.max(newData.temperature))
      .reduce( new MyMaxTemp() )

    value01.getSideOutput(new OutputTag[SensorReading]("late-data")).print("late")

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



class MyMaxTemp() extends ReduceFunction[SensorReading]{
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading =
    SensorReading(value1.id, value2.timestamp + 1, value1.temperature.max(value2.temperature))
}
