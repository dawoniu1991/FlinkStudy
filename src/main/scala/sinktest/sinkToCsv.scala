package sinktest
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode
import  org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink

object sinkToCsv {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val ds1: DataStream[Map[Int, String]] = env.fromElements(Map(1 -> "spark" , 2 -> "flink", 6 -> "scala"))
    val ds1: DataStream[String] = env.fromElements("hadoop","spark","pyspark", "java")
    //写入到本地，文本文档,NO_OVERWRITE模式下如果文件已经存在，则报错，OVERWRITE模式下如果文件已经存在，则覆盖
//    ds1.setParallelism(1).writeAsText("data/sinktest/sinkToCsv/res", WriteMode.OVERWRITE)
    val sink: StreamingFileSink[String] = StreamingFileSink
    .forRowFormat(new Path("data/sinktest/sinkToCsv/res"), new SimpleStringEncoder[String]("UTF-8")) // 所有数据都写到同一个路径
    .build()

    ds1.addSink(sink)
    env.execute()

  }
}
