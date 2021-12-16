package mytest

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStateBackend(new FsStateBackend("file:///data/StateTest/FsStateBackend", true))
    env.setStateBackend(new RocksDBStateBackend("file:///data/StateTest/RocksDBStateBackend"))

    // 开启checkpoint
    env.enableCheckpointing()

    // checkpoint的配置
    val chkpConfig = env.getCheckpointConfig
    chkpConfig.setCheckpointInterval(10000L)
    chkpConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    chkpConfig.setCheckpointTimeout(60000)
    chkpConfig.setMaxConcurrentCheckpoints(2)
    chkpConfig.setMinPauseBetweenCheckpoints(500L)
    chkpConfig.setPreferCheckpointForRecovery(true)
    chkpConfig.setTolerableCheckpointFailureNumber(0)

    // 重启策略配置
    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L))
    env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.minutes(5), Time.seconds(10)))

    // 读取数据
    //    val inputPath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    //    val inputStream = env.readTextFile(inputPath)

    val inputStream = env.socketTextStream("localhost", 7777)

    // 先转换成样例类类型（简单转换操作）
    val dataStream = inputStream
      .map( data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      } )
      .uid("1")

    // 需求：对于温度传感器温度值跳变，超过10度，报警
    val alertStream = dataStream
      .keyBy(_.id)
            .flatMap( new TempChangeAlert(10.0) )
//      .flatMapWithState[(String, Double, Double), Double] {
//        case (data: SensorReading, None) => ( List.empty, Some(data.temperature) )
//        case (data: SensorReading, lastTemp: Some[Double]) => {
//          // 跟最新的温度值求差值作比较
//          val diff = (data.temperature - lastTemp.get).abs
//          if( diff > 10.0 )
//            ( List((data.id, lastTemp.get, data.temperature)), Some(data.temperature) )
//          else
//            ( List.empty, Some(data.temperature) )
//        }
//      }

    alertStream.print()

    env.execute("state test")
  }
}



// 实现自定义RichFlatmapFunction
class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{
  // 定义状态保存上一次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  lazy val flagState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("flag", classOf[Boolean]))

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上次的温度值
    val lastTemp = lastTempState.value()
    // 跟最新的温度值求差值作比较
    val diff = (value.temperature - lastTemp).abs
//    if( !flagState.value() && diff > threshold )
    if(  diff > threshold )
//      out.collect( value.id, lastTemp, value.temperature)
      out.collect( (value.id, lastTemp, value.temperature))

    // 更新状态
    lastTempState.update(value.temperature)
//    flagState.update(true)
  }
}
