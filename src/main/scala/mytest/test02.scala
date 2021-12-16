package mytest

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.scala._

object test02 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[String] = env.readTextFile("F:\\myflinkstudy\\flinktest\\src\\main\\resources\\sensor.txt")

    val sinkRow = StreamingFileSink
      .forRowFormat(new Path("D:\\idea_out\\rollfilesink"), new SimpleStringEncoder[String]("UTF-8"))
      .withBucketAssigner(new DateTimeBucketAssigner[String]("yyyy-MM-dd/HH/mm"))
      // .withBucketCheckInterval(60 * 60 * 1000l) // 1 hour
      .build()

    dataStream.addSink(sinkRow)

    dataStream.print()

    env.execute("kafka sink test")

  }
}
