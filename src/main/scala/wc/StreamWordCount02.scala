package wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

//windows中打开bash  执行 nc -lk 7777 发送数据给flink读取
// 打开run configurations中的program arguments 写上 --host localhost --port 7777 进行配置
object StreamWordCount02 {
  def main(args: Array[String]): Unit = {

    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = tool.get("host")
    println(host)
    val port: Int = tool.getInt("port")
    println(port)
    println("==========being===========")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val value01: DataStream[String] = env.socketTextStream(host, port)

    val value02: DataStream[(String, Int)] = value01.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    value02.print().setParallelism(1)

    env.execute("stream word count02")


  }
}
