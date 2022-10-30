package TwoPhaseCommitTest

/**
 * @author jiangfan
 * @date 2022/9/27 18:15
 */
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink

import java.util.Properties
import scala.util.Random

object FlinkPipeline {
  def main(args: Array[String]): Unit = {
    println("Starting the pipeline...")
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(60000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)
    val CheckpointingDir="file:///F:/myflinkstudy/FlinkStudy/data/apitest/StateTest"
    val backend = new RocksDBStateBackend(CheckpointingDir, true)
    env.setStateBackend(backend.asInstanceOf[StateBackend])
    // 设置模式为精确一次 (这是默认值)
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.setRestartStrategy(RestartStrategies.noRestart)

//    val input : DataStream[String] = env.socketTextStream("localhost",6666)
val tupleStream: DataStream[(String, String, String)] = env.addSource(new MySource)
//    (11,22,33,44)
//    tupleStream.print("res===")
//    val fileSink = StreamingFileSink
//      .forRowFormat(new Path("data/TwoPhaseCommitTest/FlinkPipeline/output"), new SimpleStringEncoder[String]("UTF-8"))
//      .build()
//    tupleStream.map(_.toString).addSink( fileSink )
    val properties : Properties = new Properties()
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    properties.setProperty("url","jdbc:mysql://localhost:3306/mytest_db")
    properties.setProperty("user","root")
    properties.setProperty("password","123456")

//    tupleStream.addSink(new PostgreSink(properties,env.getConfig)).name("Postgres Sink").setParallelism(1)
    //  tupleStream.writeAsText("/home/luca/Desktop/output",FileSystem.WriteMode.OVERWRITE).name("File Sink").setParallelism(1)
//        tupleStream.addSink(new MysqlSink(env.getConfig)).name("mysql Sink").setParallelism(1)
        tupleStream.addSink(new MysqlSinkDruid01(env.getConfig)).name("mysql Sink").setParallelism(1)
//        tupleStream.addSink(new MysqlSinkDruid02(env.getConfig)).name("mysql Sink").setParallelism(1)
//        tupleStream.addSink(new MySqlTwoPhaseCommitSink02()).name("mysql Sink").setParallelism(1)
//        tupleStream.addSink(new MysqlSinkJdbc(env.getConfig)).name("mysql Sink").setParallelism(1)
//    tupleStream.map(new MapInsertToMysql).print("result===")
    env.execute()
  }

  class MySource() extends SourceFunction[(String,String,String)]{
    var running:Boolean=true
    override def run(ctx: SourceFunction.SourceContext[(String,String,String)]): Unit = {
      val rand = new Random()
      println("00000000000000000000000")
      while(running){
        val curTime: Long = System.currentTimeMillis()
        val data=rand.nextInt(100).toString
        ctx.collect((data,curTime.toString,"sensor"+data))
//        println("=================================================")
        Thread.sleep(500)
      }
    }

    override def cancel(): Unit = {
      running=false
    }
  }
}
