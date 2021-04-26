package mytest

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据
//    val inputStream = env.socketTextStream("localhost", 7777)

    val inputStream: DataStream[String] = env.fromElements("sensor_1,1614855666,18","sensor_2,1614855333,35.8","sensor_2,1614855000,56","sensor_1,1614855111,18","sensor_1,1614855113,12","sensor_1,1614855200,68","sensor_1,1614855200,23")

    // 先转换成样例类类型（简单转换操作）
    val dataStream = inputStream
      .map( data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      } )

//    val highTempStream = dataStream
//      .process( new SplitTempProcessor(30.0) )
//    highTempStream.print("high")
//    highTempStream.getSideOutput(new OutputTag[(String, Long, Double)]("low")).print("low")

    val highTempStream = dataStream
      .process( new SplitTempProcessor01(30.0,20.0) )
    highTempStream.print("high")
    val value01: DataStream[(String, Long, Double)] = highTempStream.getSideOutput(new OutputTag[(String, Long, Double)]("middle"))
      value01.print("middle")
    val value02: DataStream[Double] = highTempStream.getSideOutput(new OutputTag[Double]("low"))
      value02.print("low")

    env.execute("side output test")
  }
}


// 实现自定义ProcessFunction，进行分流
class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading]{
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if( value.temperature > threshold ){
      // 如果当前温度值大于30，那么输出到主流
      out.collect(value)
    } else {
      // 如果不超过30度，那么输出到侧输出流
      ctx.output(new OutputTag[(String, Long, Double)]("low"), (value.id, value.timestamp, value.temperature))
    }
  }
}



// 实现自定义ProcessFunction01，进行分流
class SplitTempProcessor01(max: Double,min:Double) extends ProcessFunction[SensorReading, SensorReading]{
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if( value.temperature > max ){
      // 如果当前温度值大于30，那么输出到主流
      out.collect(value)
    } else if(  value.temperature <=max  && value.temperature >=min) {
      // 如果不超过30度，那么输出到侧输出流
      ctx.output(new OutputTag[(String, Long, Double)]("middle"), (value.id, value.timestamp, value.temperature))
    }else {
      // 如果不超过30度，那么输出到侧输出流
      ctx.output(new OutputTag[ Double]("low"),  value.temperature)
    }
  }
}