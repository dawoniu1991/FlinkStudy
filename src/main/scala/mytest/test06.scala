package mytest

import apitest.SensorReading
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.contrib.streaming.state.{PredefinedOptions, RocksDBStateBackend}
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import java.util.concurrent.TimeUnit
import scala.collection.immutable
import scala.util.Random

object test06 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    println("kaishi======")

    env.enableCheckpointing(300000L)
      .setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000L * 60L * 5L)
    env.getCheckpointConfig.setCheckpointTimeout(1000L * 60L * 60L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    val CheckpointingDir="file:///F:/myflinkstudy/FlinkStudy/data/mytest/test06/checkpoint"
    val backend = new RocksDBStateBackend(CheckpointingDir, true)
    backend.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED)
    import org.apache.flink.streaming.api.CheckpointingMode
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.setStateBackend(backend.asInstanceOf[StateBackend])
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
      org.apache.flink.api.common.time.Time.of(10, TimeUnit.MINUTES),
      org.apache.flink.api.common.time.Time.of(15, TimeUnit.SECONDS)))

//        val stream01: DataStream[SensorReading] = env.addSource(new SensorSource01())
        val stream01: DataStream[String] = env.addSource(new SensorSource02())
//        stream01.print("stream01").setParallelism(1)
    val stream02 = stream01.filter(_.contains("123")).map(x =>{
      println("x="+x)
      x+"qqq"
    } ).name("filter001")
    val outpath = "data/mytest/test06/res"
//    1存储方式
//    val sink: StreamingFileSink[String] = StreamingFileSink
//      .forRowFormat(new Path(outpath), new SimpleStringEncoder[String]("UTF-8")) // 所有数据都写到同一个路径
//      .build()
//    2存储方式
//    val sink: StreamingFileSink[String] = StreamingFileSink
//      .forRowFormat(new Path(outpath), new SimpleStringEncoder[String]("UTF-8"))
//      .withRollingPolicy(
//        DefaultRollingPolicy.create()
//          .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
//          .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
//          .withMaxPartSize(1024 * 1024 * 1024)
//          .build())
//      .build()
//     3存储方式
    val sink = StreamingFileSink.forRowFormat(new Path(outpath), new SimpleStringEncoder[String]("UTF-8"))
      .withBucketAssigner(new DateTimeBucketAssigner[String]("yyyy-MM-dd/HH/mm")) //设置时间的桶
      .withRollingPolicy(new CustomRollingPolicy)
      .build()

    stream02.addSink(sink).name("myfilesink")
    env.execute("source test")
  }
}


class SensorSource02() extends SourceFunction[String]{
  var running:Boolean=true
//  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val rand = new Random()
    var curTemp: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(
      i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
    )

    println("0000000099999999")
    while(running){
      curTemp= curTemp.map(
        t => ( t._1,t._2+rand.nextGaussian())
      )
      val curTime: Long = System.currentTimeMillis()
      curTemp.foreach(
      //        t=>ctx.collect(SensorReading(t._1,curTime,t._2))
      //        t=>ctx.collect(SensorReading(t._1,curTime,t._2).toString)
        t =>   {
          val str = (t._1, curTime, t._2).toString()
//          println(str)
         ctx.collect(str)
        }
      )
//      println("=================================================")
      Thread.sleep(500)
    }
  }

  override def cancel(): Unit = {
    running=false
  }
}
