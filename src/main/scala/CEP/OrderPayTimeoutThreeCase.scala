package CEP

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.datastream.DataStreamSource

import java.util
import java.time.Duration

// user_1, create, order_1, 1
// user_1, modify, order_1, 2
// user_1, pay,    order_1, 3
// user_2, create, order_3, 4
// user_2, modify,   order_3, 6
// user_2, pay,   order_3, 7
// user_3, create, order_4, 10
// user_3, modify, order_4, 11
// user_3, pay, order_4, 12
// user_4, modify, order_4, 16
// user_4, pay, order_4, 17
// user_4, create, order_4, 15
// user_5, create, order_4,20
// user_5, modify, order_4,21
// user_5, pay, order_4,22

// user_6, modify, order_4,25
// user_6, create, order_4,23
// user_6, pay, order_4,26

// user_7, create, order_4,27
// user_7, modify, order_4,33
// user_7, pay, order_4,34

// user_8, pay, order_4,50
// user_8, modify, order_4,45
// user_8, create, order_4,42

// user_9, pay, order_4,62
// user_9, create, order_4,60
// user_9, modify, order_4,61


// user_19, modify, order_4,66
// user_19, create, order_4,61
// user_19, pay, order_4,69

// user_520, create, order_4,72
// user_520, modify, order_4,73
// user_520, pay, order_4,75

// user_1200, create, order_4,78
// user_1200, modify, order_4,79
// user_1200, pay, order_4,85

// user_1500, create, order_4,88
// user_1500, modify, order_4,89
// user_1500, pay, order_4,90

// user_1600, pay, order_4,99
// user_1600, modify, order_4,94
// user_1600, create, order_4,92

// user_1800, create, order_4,103

// user_2000, pay, order_4,106
// user_2000, modify, order_4,105
// user_2000, create, order_4,104

// user_2100, create, order_4,110

// user_3000, pay, order_4,116
// user_3000, modify, order_4,115
// user_3000, create, order_4,112

// user_3100, create, order_4,120

// user_4000, pay, order_4,126
// user_4000, modify, order_4,125
// user_4000, create, order_4,123

// user_4100, create, order_4,130
object OrderPayTimeoutThreeCase {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
env.setParallelism(1)
    val input: DataStream[String] = env.socketTextStream("localhost", 6666)
    val orderEventStream=input.map(line=>{
      val strings = line.split(",")
      OrderEvent(strings(0).trim,strings(1).trim,strings(2).trim,strings(3).trim.toLong)
    }).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(0))
            .withTimestampAssigner(new SerializableTimestampAssigner[OrderEvent] {
              override def extractTimestamp(t: OrderEvent, l: Long): Long = t.timestamp*1000L
            }))
      .keyBy(_.orderId)

//    val orderEventStream = env.fromElements(
//      OrderEvent("user_1",  "create",   "order_1", 1000L),
//      OrderEvent("user_1",  "modify",   "order_1", 10 * 1000L),
//      OrderEvent("user_1",  "pay",      "order_1", 60 * 1000L),
//      OrderEvent("user_2",  "create",   "order_3", 10 * 60 * 1000L),
//      OrderEvent("user_2",  "pay",      "order_3", 20 * 60 * 1000L),
//      OrderEvent("user_3",  "create",   "order_4", 30 * 60 * 1000L),
//      OrderEvent("user_3",  "modify",   "order_4", 40 * 60 * 1000L),
////    OrderEvent("user_3",  "create",   "order_4", 50 * 60 * 1000L),
//      OrderEvent("user_4",  "create",      "order_5", 5899999L),
//        OrderEvent("user_3",  "pay",      "order_4", 2699999L)
//      //      OrderEvent("user_5",  "create",   "order_6", 50 * 60 * 1000L),
////      OrderEvent("user_5",  "modify",   "order_6", 51 * 60 * 1000L),
////      OrderEvent("user_5",  "pay",      "order_6", 52 * 60 * 1000L),
//    ).assignAscendingTimestamps( _.timestamp  )
//      .keyBy(_.orderId)

//    .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))
    //      .withTimestampAssigner(new SerializableTimestampAssigner[OrderEvent] {
    //        override def extractTimestamp(t: OrderEvent, l: Long): Long = t.timestamp
    //      }))

    // 从文件中读取数据
//    val resource = getClass.getResource("/OrderLog.csv")
//    val orderEventStream = env.readTextFile(resource.getPath)
//      .map( line => {
//        val arr = line.split(",")
//        OrderEvent(arr(0), arr(1), arr(2), arr(3).toLong)
//      } )
//      .assignAscendingTimestamps( _.timestamp * 1000L )

    // 1. 定义一个匹配模式
    val orderPayPattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("modify").where(_.eventType == "modify")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.seconds(10))

    // 2. 将模式应用在数据流上，进行复杂事件序列的检测
    val patternStream = CEP.pattern(orderEventStream.keyBy(_.orderId), orderPayPattern)

    // 3. 定义一个侧输出流标签，用于将超时事件输出
    val timeoutOutputTag = new OutputTag[OrderPayResult]("timeout")
    val timeoutTag = new OutputTag[OrderEvent]("order-event")

    // 4. 检出复杂事件，并转换输出结果
    val resultStream = patternStream.sideOutputLateData(timeoutTag).select( timeoutOutputTag, new OrderPayTimeoutSelect01(), new OrderPaySelect01() )

    resultStream.print("payed")
    resultStream.getSideOutput(timeoutOutputTag).print("timeout")
    resultStream.getSideOutput(timeoutTag).print("超时")

    env.execute("order pay timeout job")
  }
}

// 自定义PatternSelectFunction
class OrderPaySelect01() extends PatternSelectFunction[OrderEvent, OrderPayResult]{
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderPayResult = {
//    val createOrderId = pattern.get("create").iterator().next().orderId
//    val modifyOrderId = pattern.get("modify").iterator().next().orderId
//    val payedOrderId = pattern.get("pay").iterator().next().orderId

    val orderId = pattern.get("create").iterator().next().orderId
    val createOrderId = pattern.get("create").iterator().next().timestamp
    val modifyOrderId = pattern.get("modify").iterator().next().timestamp
    val payedOrderId = pattern.get("pay").iterator().next().timestamp
    OrderPayResult(orderId+":"+createOrderId+"--"+modifyOrderId+"=="+payedOrderId, "payed successfully~~")
  }
}

// 自定义PatternTimeoutFunction
class OrderPayTimeoutSelect01() extends PatternTimeoutFunction[OrderEvent, OrderPayResult]{
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderPayResult = {
    val timeoutOrderId = pattern.get("create").iterator().next().orderId
    if(pattern.get("modify")!=null){
      val modifyTimeoutOrderId = pattern.get("modify").iterator().next().orderId
      println(modifyTimeoutOrderId)
      println("cunzai-modify")
      OrderPayResult(timeoutOrderId, s"有第一，二步，没有第三步，order timeout at $timeoutTimestamp")
    }else{
      OrderPayResult(timeoutOrderId, s"只有第一步，没有二三步，order timeout at $timeoutTimestamp")
    }
  }
}