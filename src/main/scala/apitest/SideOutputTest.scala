package apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import sourcetest.SensorReading

object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val streamFile: DataStream[String] = env.socketTextStream("localhost", 7777)

    val dataStream: DataStream[SensorReading] = streamFile.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)

    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp*1000
      })



    val value1= dataStream.process(new FreezingAlert())


    value1.print("processed data")
value1.getSideOutput(new OutputTag[String]("ezing alert")).print("alert data")

    env.execute("window test")
  }
}


class FreezingAlert() extends ProcessFunction[SensorReading,SensorReading]{
  lazy val alertOutput:OutputTag[String]=new OutputTag[String]("ezing alert")
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if(value.temperature<32.0){
      ctx.output(alertOutput,"freezing alert for ====="+value.id)
    }else{
      out.collect(value)
    }

  }
}