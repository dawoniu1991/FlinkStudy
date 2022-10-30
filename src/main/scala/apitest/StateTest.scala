package apitest

/**
 * @author jiangfan
 * @date 2021/12/16 16:59
 */
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import sourcetest.SensorReading

/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 *
 * Project: FlinkTutorial
 * Package: com.atguigu.api
 * Version: 1.0
 *
 * Created by wushengran on 2020/9/1 16:58
 */
object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    //    env.setStateBackend( new FsStateBackend("") )
    val CheckpointingDir="file:///F:/myflinkstudy/FlinkStudy/data/apitest/StateTest"
    val backend = new RocksDBStateBackend(CheckpointingDir, true)
    env.setStateBackend(backend.asInstanceOf[StateBackend])

    // 容错机制相关配置
    // checkpoint
    env.enableCheckpointing(1000L)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout( 60000L )
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)

    // 重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 10000L))

    // 读取数据
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    val dataStream = inputStream
      .map(line => {
        val arr = line.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      })

    val warningStream = dataStream
      .keyBy( _.id )     // 以id分组
            .flatMap( new TempChangeWarning(10.0) )
//      .flatMapWithState[(String, Double, Double), Double](
//        {
//          case (inputData: SensorReading, None) => (List.empty, Some(inputData.temperature))
//          case (inputData: SensorReading, lastTemp: Some[Double]) =>
//            val diff = (inputData.temperature - lastTemp.get).abs
//            if( diff > 10.0 ){
//              ( List((inputData.id, lastTemp.get, inputData.temperature)), Some(inputData.temperature) )
//            } else {
//              ( List.empty, Some(inputData.temperature) )
//            }
//        }
//      )

    warningStream.print()

    env.execute("state test job")
  }
}

// 自定义RichFlatMapFunction，实现温度跳变检测报警功能
class TempChangeWarning(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{
  // 定义状态，保存上一个温度值
  private var lastTempState: ValueState[Double] = _


  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState( new ValueStateDescriptor[Double]("last-temp", classOf[Double]) )
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 从状态中获取上次温度值
    val lastTemp = lastTempState.value()

    // 跟当前温度作比较，如果大于阈值，输出报警信息
    val diff = (value.temperature - lastTemp).abs
    //    if( diff > threshold && lastTemp != defaultTemp ){
    if(  diff > threshold ){
      out.collect( (value.id, lastTemp, value.temperature) )
    }

    // 更新状态
    lastTempState.update(value.temperature)
  }
}