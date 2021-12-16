package mytest

//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector



case class Word(word: String)

object test03 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    val path = this.getClass.getResource("/data/wc.txt").getPath
    val path="data\\test03\\input"


    val stream: KeyedStream[Word, String] = env.readTextFile(path).flatMap(_.split(" "))
      .map(Word(_))
      .keyBy(_.word) //按照word字段分区

    stream.process(new KeyedProcessFunction[String,Word, String] {
      override def processElement(value: Word, ctx: KeyedProcessFunction[String, Word, String]#Context, out: Collector[String]): Unit = {
        val rowkey = ctx.getCurrentKey //获取每一行的key
        val rowvalue = value // 获取每一行的value
        val output_value = s"key=${rowkey}###value=$rowvalue" //定义新的输出行
        out.collect(output_value)
      }
    }).print()


    env.execute("output value")
  }
}
