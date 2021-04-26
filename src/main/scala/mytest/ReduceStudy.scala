package mytest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.contrib.streaming.state.{PredefinedOptions, RocksDBStateBackend}
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import scala.collection.immutable
import scala.util.Random

/**
 * @author jiangfan
 * @date 2021/4/25 14:13
 */
object ReduceStudy {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    println("kaishi======")

    env.enableCheckpointing(60000L)
      .setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000L * 60L * 1L)
    env.getCheckpointConfig.setCheckpointTimeout(1000L * 60L * 10L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    val CheckpointingDir="file:///F:/myflinkstudy/FlinkStudy/data/mytest/ReduceStudy/checkpoint"
    val backend = new RocksDBStateBackend(CheckpointingDir, true)
    backend.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED)
    import org.apache.flink.streaming.api.CheckpointingMode
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.setStateBackend(backend.asInstanceOf[StateBackend])
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
      org.apache.flink.api.common.time.Time.of(10, TimeUnit.MINUTES),
      org.apache.flink.api.common.time.Time.of(15, TimeUnit.SECONDS)))

    //        val stream01: DataStream[SensorReading] = env.addSource(new SensorSource01())
    val stream01: DataStream[(String,Long,Int)] = env.addSource(new SensorSource01())
    //        stream01.print("stream01").setParallelism(1)
    val stream02: DataStream[String] = stream01
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.minutes(3)))
      .reduce(new UnifiedReduce, new MetricKeyedProcess)
      .uid("final-reduce").name("final-reduce")


//    val stream02 = stream01.filter(_.contains("123")).map(x =>{
//      println("x="+x)
//      x+"qqq"
//    } ).name("filter001")

    val outpath = "data/mytest/ReduceStudy/res"
    val sink = StreamingFileSink.forRowFormat(new Path(outpath), new SimpleStringEncoder[String]("UTF-8"))
      .withBucketAssigner(new DateTimeBucketAssigner[String]("yyyy-MM-dd/HH/mm")) //设置时间的桶
      .withRollingPolicy(new CustomRollingPolicy)
      .build()

    stream02.addSink(sink).name("myfilesink")
    env.execute("source test")
  }
}

class UnifiedReduce extends ReduceFunction[(String,Long,Int)] {
  override def reduce(value1: (String, Long, Int), value2: (String, Long, Int)): (String, Long, Int) = {
    println("value1===="+value1)
    println("value2===="+value2)
    (value1._1,value1._2+value2._2,value1._3+value2._3)
  }
}


class MetricKeyedProcess extends ProcessWindowFunction[(String, Long, Int),
  String, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[(String, Long, Int)], out: Collector[String]): Unit = {
    println("size=========="+elements.size)
    val head: (String, Long, Int) = elements.head
    println("head===="+head)
    val dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateStr = dateformat.format(System.currentTimeMillis)
    println("time===="+dateStr)
    out.collect(head.toString())
  }
}


class SensorSource01() extends SourceFunction[(String,Long,Int)]{
  var running:Boolean=true
  //  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
//  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
  override def run(ctx: SourceFunction.SourceContext[(String,Long,Int)]): Unit = {
    val rand = new Random()
    var curTemp: immutable.IndexedSeq[(String, Int)] = 1.to(1).map(
      i => ("sensor_" + i, 60  )
    )

    println("0000000099999999")
    while(running){
      val curTemp01= curTemp.map(
        t => ( t._1,t._2+rand.nextInt(10))
      )
      val curTime: Long = System.currentTimeMillis()
      curTemp01.foreach(
        //        t=>ctx.collect(SensorReading(t._1,curTime,t._2))
        //        t=>ctx.collect(SensorReading(t._1,curTime,t._2).toString)
        t =>   {
          val str = (t._1, curTime, t._2)
          //          println(str)
          ctx.collect(str)
        }
      )
      //      println("=================================================")
      Thread.sleep(10000)
    }
  }

  override def cancel(): Unit = {
    running=false
  }
}
