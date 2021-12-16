package wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

//windows中打开bash  执行 nc -lk 7777 发送数据给flink读取
object StreamWordCount01 {
  def main(args: Array[String]): Unit = {

    println("==========being===========")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val value01: DataStream[String] = env.socketTextStream("localhost", 7777)
    val value02: DataStream[(String, Int)] = value01.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    value02.print()
    env.execute("stream word count")
  }
}
