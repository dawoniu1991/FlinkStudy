package mytest

import java.lang

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.{DataStream, _}

object SplitSelect {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val elements: DataStream[Int] = env.fromElements(1,2,3,4,5,6)
    //数据分流

//    方式一
//    val split_data = elements.split(
//      (num: Int) => (num % 2) match {
//        case 0 => List("even")
//        case 1 => List("odd")
//      }
//    )
//   方式二
//    val split_data = elements.split(
//      num => {if (num % 2==0) Seq("even") else Seq("odd")}
//    )
//    方式三
    val split_data = elements.split( new OutputSelector[Int] {
  override def select(out: Int): lang.Iterable[String] = {
    import java.util
    val output = new util.ArrayList[String]
    out % 3 match {
      case 0 => output.add("even")
      case 1 => output.add("odd")
      case _ => // drop it
    }
    output
  }
}

    )

    //获取分流后的数据
    val select: DataStream[Int] = split_data.select("even")
    select.print("qq======")

    val select02: DataStream[Int] = split_data.select("odd")
    select02.print("wwwww======")
    env.execute("aaa=")

    //    val value02: SplitStream[SensorReading] = value01.split(data => {
    //      if (data.temperature > 30) Seq("high") else Seq("low")
    //    })
    //
    //    val high: DataStream[SensorReading] = value02.select("high")

  }
}
