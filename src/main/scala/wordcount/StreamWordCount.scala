package wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object StreamWordCount {
  def main(args: Array[String]): Unit = {

    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = tool.get("host")
    val port: Int = tool.getInt("port")
    println("==========being===========")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.disableOperatorChaining()

//    val value01: DataStream[String] = env.socketTextStream("localhost", 7777)
    val value01: DataStream[String] = env.socketTextStream(host, port)

    val value02: DataStream[(String, Int)] = value01.flatMap(_.split(" "))
//      .filter(_.nonEmpty).disableChaining()
      .filter(_.nonEmpty)
//      .map((_, 1)).startNewChain()
      .map((_, 1))
      .keyBy(0)
      .sum(1)

//    value02.print()
    value02.print().setParallelism(1)

    env.execute("stream word count")


  }
}
