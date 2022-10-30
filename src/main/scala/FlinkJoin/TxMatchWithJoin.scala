package FlinkJoin

import CEP.OrderEvent
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


object TxMatchWithJoin {
  case class Event(user: String, payChannel: String, timestamp: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 订单事件流
    val orderStream: DataStream[(String, String, Long)] = env
      .fromElements(
        ("Mary", "order-1", 5000L),
        ("Alice", "order-2", 5000L),
        ("Bob", "order-3", 20000L),
        ("Alice", "order-4", 20000L),
        ("Cary", "order-5", 51000L)
      ).assignAscendingTimestamps(_._3)

    // 点击事件流
    val pvStream: DataStream[Event] = env
      .fromElements(
        Event("Bob", "./cart", 2000L),
        Event("Alice", "./prod?id=100", 3000L),
        Event("Alice", "./prod?id=200", 3500L),
        Event("Bob", "./prod?id=2", 2500L),
        Event("Alice", "./prod?id=300", 36000L),
        Event("Bob", "./home", 30000L),
        Event("Bob", "./prod?id=1", 23000L),
        Event("Bob", "./prod?id=3", 33000L)
      ).assignAscendingTimestamps(_.timestamp)

    // 两条流进行间隔联结，匹配一个订单事件前后一段时间内发生的点击事件
    orderStream.keyBy(_._1)
      .intervalJoin(pvStream.keyBy(_.user))
      .between(Time.seconds(-5), Time.seconds(10))
      .process(new ProcessJoinFunction[(String, String, Long), Event, String] {
        override def processElement(left: (String, String, Long), right: Event, ctx: ProcessJoinFunction[(String, String, Long), Event, String]#Context, out: Collector[String]): Unit = {
          out.collect(left + "=>" + right)
        }
      })
      .print()

    env.execute()
  }
}

