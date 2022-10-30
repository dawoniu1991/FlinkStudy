package com.work

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.lang
import java.text.SimpleDateFormat

/**
 * @author jiangfan
 * @date 2022/6/19 15:51
 */
object Job03 {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)    // 可以在本地cmd，然后bash  nc -lk 7777 开启端口发送数据进行访问
        val inputStream: DataStream[String] = env.readTextFile("src\\main\\resources\\job01.csv")

    val dataStream: DataStream[(String, String, Int)] = inputStream
      .map( line => {
        val arr = line.split(",")
        Tuple3( arr(0), arr(1), arr(2).toInt )
      } ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, String, Int)](Time.seconds(0)) {
      override def extractTimestamp(element: (String, String, Int)): Long = funStringToTimeStamp(element._2)
    }
    )




    dataStream.keyBy(_._1).process(new averagrTempProcessFun()).print("res=")
    env.execute("job03=====")


  }

  def funStringToTimeStamp(time: String): Long = {
    val fm = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val dt = fm.parse(time)
    dt.getTime
  }

}

class averagrTempProcessFun()  extends KeyedProcessFunction[String,(String, String, Int),(String, String, Double)]{

  lazy val counter: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("counter",classOf[Long]))
  lazy val sumTemp: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("sum-Temp",classOf[Long]))
  lazy val currTemp: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("curr-Temp",classOf[Long]))
  lazy val triggerTime: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("trigger-Time",classOf[Long]))


  override def processElement(value: (String, String, Int), ctx: KeyedProcessFunction[String, (String, String, Int), (String, String, Double)]#Context, out: Collector[(String, String, Double)]): Unit = {
    //存储当前温度值
    currTemp.update(value._3)
    if(counter.value()==null){
      counter.update(1)
    }else{
      counter.update(counter.value()+1)
    }

    if(sumTemp.value()==null){
      sumTemp.update(value._3)
    }else{
      sumTemp.update(sumTemp.value()+value._3)
    }

    val nowtime: lang.Long = ctx.timestamp()
    println("time===="+nowtime)
    //获取每天凌晨00：00：00的时间戳，在凌晨00:00:00的时候触发计算，算出当天温度平均值
    val trigger=getEndDateTimestamp(nowtime)
    println("trigger================"+trigger)

    if(triggerTime.value()==null){
      triggerTime.update(trigger)
      ctx.timerService().registerEventTimeTimer(trigger)
    }
    //防止前一天的过期数据触发计算
    if(trigger>triggerTime.value() ){
      triggerTime.update(trigger)
      ctx.timerService().registerEventTimeTimer(trigger)
    }
  }


  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, String, Int), (String, String, Double)]#OnTimerContext, out: Collector[(String, String, Double)]): Unit = {
    var averageTemp=0d
    println("sum=="+sumTemp.value())
    println("count=="+counter.value())

    if( sumTemp.value()!=null && counter.value()!=null ){
         averageTemp=(sumTemp.value()-currTemp.value()).toDouble/(counter.value()-1L)
    }
    out.collect((ctx.getCurrentKey,ctx.timestamp().toString,averageTemp))
    sumTemp.update(currTemp.value())
    counter.update(1L)
  }

  private def getEndDateTimestamp(curTime:Long) = {
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val hourTime = df.format(curTime)
    val hourTimestamp = df.parse(hourTime).getTime
    hourTimestamp + 86400000
  }


}


