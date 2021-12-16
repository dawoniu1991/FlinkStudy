package mytest

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import scala.collection.immutable
import scala.util.Random

//订单对象(userid、消费总额total)
case class Order01(userid: Long, total: Long)
case class OrderSummary01(startTime: String, endTime: String, userid: Long, total: Long)

object IngestionOrProcessTime01  {
  def main(args: Array[String]): Unit = {

    val streamenv = StreamExecutionEnvironment.getExecutionEnvironment
    //  streamenv.setParallelism(1)
    streamenv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val input = streamenv.addSource(new IngestionOrProcessTimeSource01())

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
          println("accumulator._1==" + accumulator._1 + "accumulator._2==" + accumulator._2)
          (value.userid, accumulator._2 + value.total)
        }

        override def getResult(accumulator: (Long, Long)) = {
          println("2222222222222222222222222getResult")
          println("getResult======" + System.currentTimeMillis())
          accumulator
        }

        //合并累加器
        override def merge(a: (Long, Long), b: (Long, Long)) = {
          println("3333333333333333333333333333merge")
          (a._1, a._2 + b._2)
        }
      }
        ,
        new WindowFunction[(Long, Long), OrderSummary, Long, TimeWindow]() {
          override def apply(key: Long, window: TimeWindow, inputs: Iterable[(Long, Long)], out: Collector[OrderSummary]) {
            println("aplly==========" + System.currentTimeMillis())
            val date1 = new java.util.Date();
            date1.setTime(window.getStart)
            val date2 = new java.util.Date();
            date2.setTime(window.getEnd)
            val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
            val winStartTime = simpleDateFormat.format(date1);
            val winEndTime = simpleDateFormat.format(date2)
            for (value <- inputs) {
              //inputs已经按key集合，有几个key，就应该循环几次
              out.collect(OrderSummary("winStartTime :" + winStartTime, "winEndTime :" + winEndTime, value._1, value._2))
            }
          }
        }
      )
      .print()

    streamenv.execute("IngestionOrProcessTime_starting")
  }
}

class IngestionOrProcessTimeSource01() extends SourceFunction[Order]{
  var running:Boolean=true
  //  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
  override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
    val rand = new Random()
    var curTemp: immutable.IndexedSeq[(Int, Int)] = 1.to(3).map(
      i => ( i, 60 + rand.nextInt(10) * 20)
    )

    println("0000000099999999")
    while(running){
      curTemp= curTemp.map(
        t => ( t._1,1)
      )
      val curTime: Long = System.currentTimeMillis()
      curTemp.foreach(
        //        t=>ctx.collect(SensorReading(t._1,curTime,t._2))
        //        t=>ctx.collect(SensorReading(t._1,curTime,t._2).toString)
        t =>   {
          //          val str = (t._1, curTime, t._2).toString()
          val str = Order(t._1.toLong, t._2.toLong)
          //                              println(str)
          ctx.collect(str)
        }
      )
      //      println("=================================================")
      Thread.sleep(500)
    }
  }

  override def cancel(): Unit = {
    running=false
  }
}