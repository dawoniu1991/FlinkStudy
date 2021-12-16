package mytest

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat

//订单对象(userid、消费总额total)
case class Order(userid: Long, total: Long)
case class OrderSummary(startTime: String, endTime: String, userid: Long, total: Long)

object IngestionOrProcessTime extends App {

  val streamenv = StreamExecutionEnvironment.createLocalEnvironment()
  streamenv.setParallelism(1)
  //第一：设置IngestionTime，很明显这四条数据，将同一时间进入到Flink系统，将在同一个窗口中计算，按userid为key做聚合
  //  streamenv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

  //第二：设置ProcessingTime，在aggregate方法中，加一个针对时间的操作，否则将无数据
  streamenv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

  //基于key做聚合即可
//  val input=streamenv.fromElements(Order(100L, 1000L), Order(101L, 2000L), Order(100L, 3000L), Order(101L, 4000L))

  val input = streamenv.socketTextStream("localhost",6666).map(line => Order(line.split(",",-1)(0).toLong,line.split(",",-1)(1).toLong)).setParallelism(2)

    input
    .keyBy(_.userid)
    .timeWindow(Time.seconds(10))
    .aggregate(new AggregateFunction[Order, (Long, Long), (Long, Long)] {
      //创建累加器
      override def createAccumulator() = {
        //Thread.sleep(5000)  //每天event睡5秒，若是设置IngestionTime，此处不影响，若为ProcessingTime才有影响
        println("createAccumulator000000000000000")
//        Thread.sleep(10000)  //对窗口的影响
        (0L, 0L)
      }
      //累加器内累加
      override def add(value: Order, accumulator: (Long, Long)) = {
        println("1111111111111111111111111add")
        println("accumulator._1=="+accumulator._1+"accumulator._2=="+accumulator._2)
        (value.userid, accumulator._2 + value.total)
      }
      override def getResult(accumulator: (Long, Long)) = {
        println("2222222222222222222222222getResult")
        println("getResult======"+System.currentTimeMillis())
        accumulator
      }
      //合并累加器
      override def merge(a: (Long, Long), b: (Long, Long)) = {
        println("3333333333333333333333333333merge")
        (a._1, a._2 + b._2)}
       }
      ,
      new WindowFunction[(Long, Long), OrderSummary, Long, TimeWindow]() {
        override def apply(key: Long, window: TimeWindow, inputs: Iterable[(Long, Long)], out: Collector[OrderSummary]) {
          println("aplly=========="+System.currentTimeMillis())
          val date1 = new java.util.Date(); date1.setTime(window.getStart)
          val date2 = new java.util.Date(); date2.setTime(window.getEnd)
          val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
          val winStartTime = simpleDateFormat.format(date1); val winEndTime = simpleDateFormat.format(date2)
          for (value <- inputs) {
            //inputs已经按key集合，有几个key，就应该循环几次
            out.collect(OrderSummary("winStartTime :" +  winStartTime, "winEndTime :" + winEndTime, value._1, value._2))
          }
        }
      }
    )
    .print()

  streamenv.execute("IngestionOrProcessTime_starting")
}