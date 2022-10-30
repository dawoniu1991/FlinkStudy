package mytest

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

import scala.collection.immutable
import scala.util.Random

/**
 * @author jiangfan
 * @date 2022/8/12 10:21
 */
object MemoryStateBackendTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    import org.apache.flink.streaming.api.CheckpointingMode
    // 每 1000ms 开始一次 checkpoint// 每 1000ms 开始一次 checkpoint

    env.enableCheckpointing(10000)

    // 高级选项：
//    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(2)
    // 设置模式为精确一次 (这是默认值)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//
//    // 确认 checkpoints 之间的时间会进行 500 ms
//    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(10000)
//
//    // Checkpoint 必须在一分钟内完成，否则就会被抛弃
//    env.getCheckpointConfig.setCheckpointTimeout(60000)
//
//    // 同一时间只允许一个 checkpoint 进行
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    //    env.setStateBackend( new FsStateBackend("") )
    val CheckpointingDir="file:///F:/myflinkstudy/FlinkStudy/data/apitest/StateTest"
    val backend = new RocksDBStateBackend(CheckpointingDir, true)
//    env.setStateBackend(backend.asInstanceOf[StateBackend])

//5242880
//    val backend = new MemoryStateBackend(11,false)
//    val backend = new MemoryStateBackend()
//    val backend = new FsStateBackend("file:///F:/myflinkstudy/FlinkStudy/data/apitest/MemoryStateBackendTest")

    env.setStateBackend(backend.asInstanceOf[StateBackend])

    val input = env.addSource(new MemoryStateBackendTestSource01())
//    val input = env.socketTextStream("localhost", 7777)
    val output = input.keyBy(_.substring(12)).process(new MemoryStateBackendTestKeyed01())
//    input.print("qqq=")
    output.print("www=")
    env.execute()
  }
}


class MemoryStateBackendTestSource01() extends SourceFunction[String]{
  var running:Boolean=true
  //  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val rand = new Random()
    var curTemp: immutable.IndexedSeq[(Int, Int)] = 1.to(1).map(
      i => ( i, 60 + rand.nextInt(10) * 20)
    )

    println("0000000099999999")
    while(running){
      curTemp= curTemp.map(
        t => ( t._1,1)
      )
      val curTime: Long = System.currentTimeMillis()
      curTemp.foreach(
        //        t=>ctx.collect(SensorReading(t._1,curTime,t._2))
        //        t=>ctx.collect(SensorReading(t._1,curTime,t._2).toString)
        t =>   {
          //          val str = (t._1, curTime, t._2).toString()
          val str = curTime.toString
          //                              println(str)
          ctx.collect(str)
        }
      )
      //      println("=================================================")
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    running=false
  }
}

class MemoryStateBackendTestKeyed01 extends KeyedProcessFunction[String,String,String]{

  lazy val lastValueState: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("last-temp", classOf[String]))


  override def processElement(value: String, ctx: KeyedProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {

    lastValueState.update(value*13000000)
    out.collect(value)
//    out.collect(value*20)
//    out.collect(lastValueState.value())
//    out.collect(ctx.getCurrentKey)
//    out.collect((value*10000000).size.toString)
  }
}