package FlinkJoin

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author jiangfan
 * @date 2022/10/9 19:09
 */
object WindowJoinExample {
  //  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {


//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//    val orangeStream = env
//      .fromElements(("a", 1999L), ("a", 2001L),("b", 2201L))
//      .assignAscendingTimestamps(_._2)
//
//    val greenStream = env
//      .fromElements(("a", 91L), ("a", 1002L), ("a", 2999L), ("a", 2000L))
//      .assignAscendingTimestamps(_._2)
//
//    orangeStream.join(greenStream)
//      .where(r => r._1) // 第一条流使用`_1`字段做keyBy
//      .equalTo(r => r._1) // 第二条流使用`_1`字段做keyBy
//      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
//      .apply { (e1, e2) => e1 + " *** " + e2 }
//      .print()
//    env.execute()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream1 = env.fromElements(
      ("a", 1000L),
      ("b", 1000L),
      ("a", 2000L),
      ("b", 6000L)
    ).assignAscendingTimestamps(_._2)

    val stream2 = env.fromElements(
      ("a", 3000L),
      ("b", 3000L),
      ("a", 4000L),
      ("b", 8000L)
    ).assignAscendingTimestamps(_._2)

    // 窗口联结操作
    stream1.join(stream2)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply( new JoinFunction[(String, Long), (String, Long), String] {
        override def join(in1: (String, Long), in2: (String, Long)): String = {
          in1 + "->" + in2
        }
      } )
      .print()

    env.execute()
  }
}
