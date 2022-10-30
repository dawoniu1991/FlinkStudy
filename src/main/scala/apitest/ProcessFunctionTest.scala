package apitest

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import sourcetest.SensorReading

object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.enableCheckpointing(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,500))
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3,org.apache.flink.api.common.time.Time.seconds(300),org.apache.flink.api.common.time.Time.seconds(10)))

    val streamFile: DataStream[String] = env.socketTextStream("localhost", 7777)

    implicit val typeInfo = TypeInformation.of(classOf[(String,Int)])

    val dataStream: DataStream[SensorReading] = streamFile.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)

    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp*1000
      })



    val value1: KeyedStream[SensorReading, String] = dataStream.keyBy(_.id)
//    val value2 = value1.process(new TempIncreAlert())
//    val value3 = value1.process(new TempChangeAlert2(10.0))
//val value3: DataStream[(String, Double, Double)] = value1.flatMap(new TempChangeAlert(10.0))

    val value3: DataStream[(String, Double, Double)] = value1.flatMapWithState[(String, Double, Double), Double] {
case (input:SensorReading,None) => (List.empty,Some(input.temperature))
case (input:SensorReading,lastTemp:Some[Double]) =>
        val diff=(input.temperature - lastTemp.get).abs
        if(diff>10.0){
          (List((input.id,lastTemp.get,input.temperature)),Some(input.temperature))
        }else
          (List.empty,Some(input.temperature))
    }

    dataStream.print("input data")
//    value2.print("baojing data")
    value3.print("baojing data")

    env.execute("window test")
  }
}

class  TempIncreAlert() extends KeyedProcessFunction[String,SensorReading,String]{

  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))
  lazy val currentTimer:ValueState[Long]=getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer",classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {

    val perTemp: Double = lastTemp.value()
    lastTemp.update(value.temperature)
    val curTimeTs: Long = currentTimer.value()

    if(value.temperature>perTemp && curTimeTs==0){
      val timeTs: Long = ctx.timerService().currentProcessingTime() + 10000L
      ctx.timerService().registerProcessingTimeTimer(timeTs)
      currentTimer.update(timeTs)
      println("perTemp="+perTemp)
      println("value.temperature"+value.temperature)
      println("curTimeTs"+curTimeTs)
      println("11111111111111111111111111111111111111111")
    }else if(perTemp>value.temperature || perTemp==0.0){
      ctx.timerService().deleteProcessingTimeTimer(curTimeTs)
      currentTimer.clear()
      println("222222222222222222")
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {

    out.collect(ctx.getCurrentKey+"温度连续上升")
    currentTimer.clear()
  }
}



class TempChangeAlert(threshold:Double) extends  RichFlatMapFunction[SensorReading,(String,Double,Double)]{

  private var lastTempState:ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {

    val lastTemp: Double = lastTempState.value()
    val diff=(value.temperature-lastTemp).abs
    if(diff>threshold){
      out.collect(value.id,lastTemp,value.temperature)
    }
    lastTempState.update(value.temperature)
  }


}

class TempChangeAlert2(threshold:Double) extends KeyedProcessFunction[String,SensorReading,(String,Double,Double)]{
  lazy val lastTempState:ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {

    val lastTemp: Double = lastTempState.value()
    val diff=(value.temperature-lastTemp).abs
    if(diff>threshold){
      out.collect(value.id,lastTemp,value.temperature)
    }
    lastTempState.update(value.temperature)
  }
}