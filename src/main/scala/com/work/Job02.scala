package com.work

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * @author jiangfan
 * @date 2022/6/19 14:24
 */
object Job02 {
  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据并转换成样例类类型，并且提取时间戳设置watermark
//    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)  // 可以在本地cmd，然后bash  nc -lk 7777 开启端口发送数据进行访问
    val inputStream: DataStream[String] = env.readTextFile("src\\main\\resources\\job02.csv")

    val dataStream: DataStream[(String, String, String,String,String)] = inputStream
      .map( line => {
        val arr = line.split(",")
        Tuple5( arr(0), arr(1), arr(2), arr(3), arr(4))
      } )

    dataStream.keyBy(_._1).process(new oilProcessFun()).print("res=")
    dataStream.keyBy(_._1).process(new OilAverageProcessFun()).print("res=")
    env.execute("job02=====")

  }
}


class oilProcessFun extends KeyedProcessFunction[String,(String, String, String,String,String),(String, String, String)]{
//        {"AB":"酸度","AE":"粘稠度","CE":"含水量"}

  lazy val firstABTarget: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("first-AB-target",classOf[Double]))
  lazy val secondABTarget: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("second-AB-target",classOf[Double]))

  lazy val firstAETarget: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("first-AE-target",classOf[Double]))
  lazy val secondAETarget: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("second-AE-target",classOf[Double]))

  lazy val firstCETarget: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("first-CE-target",classOf[Double]))
  lazy val secondCETarget: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("second-CE-target",classOf[Double]))


  lazy val lastRecord: ValueState[(String, String, String, String, String)] = getRuntimeContext.getState(new ValueStateDescriptor[(String, String, String, String, String)]("last-Record",classOf[(String, String, String, String, String)]))

  override def open(parameters: Configuration): Unit = {
  }
  def  checkData(first:Double,second:Double,third:Double):Boolean={
      if( second>first*1.1  && third>first*1.1 ){
        true
      }else{
        false
      }
  }

  override def processElement(value: (String, String, String, String, String), ctx: KeyedProcessFunction[String, (String, String, String, String, String), (String, String, String)]#Context, out: Collector[(String, String, String)]): Unit = {
       println(value)
    // "AB":"酸度" 检测
       val nowAB = value._3.split(":")(1).toDouble
       if(firstABTarget.value()==null && secondABTarget.value()==null){
         firstABTarget.update(nowAB)
       }

       if(firstABTarget.value()!=null && secondABTarget.value()==null){
         secondABTarget.update(nowAB)
       }

       if(firstABTarget.value()!=null && secondABTarget.value()!=null){
         if(checkData(firstABTarget.value(),secondABTarget.value(),nowAB)){
           out.collect((value._1,lastRecord.value()._2,"酸度:"+secondABTarget.value() +" 第一次酸度过高"))
           out.collect((value._1,value._2,"酸度:"+nowAB +" 第二次酸度过高"))
         }
         firstABTarget.update(secondABTarget.value())
         secondABTarget.update(nowAB)
       }
//"AE","粘稠度" 检测
    val nowAE = value._4.split(":")(1).toDouble
    if(firstAETarget.value()==null && secondAETarget.value()==null){
      firstAETarget.update(nowAE)
    }

    if(firstAETarget.value()!=null && secondAETarget.value()==null){
      secondAETarget.update(nowAE)
    }

    if(firstAETarget.value()!=null && secondAETarget.value()!=null){
      if(checkData(firstAETarget.value(),secondAETarget.value(),nowAE)){
        out.collect((value._1,lastRecord.value()._2,"粘稠度:"+secondAETarget.value() +" 第一次粘稠度过高"))
        out.collect((value._1,value._2,"粘稠度:"+nowAE +" 第二次粘稠度过高"))
      }
      firstAETarget.update(secondAETarget.value())
      secondAETarget.update(nowAE)
    }

//"CE","含水量" 检测
    val nowCE = value._5.split(":")(1).toDouble
    if(firstCETarget.value()==null && secondCETarget.value()==null){
      firstCETarget.update(nowCE)
    }

    if(firstCETarget.value()!=null && secondCETarget.value()==null){
      secondCETarget.update(nowCE)
    }

    if(firstCETarget.value()!=null && secondCETarget.value()!=null){
      if(checkData(firstCETarget.value(),secondCETarget.value(),nowCE)){
        out.collect((value._1,lastRecord.value()._2,"含水量:"+secondCETarget.value() +" 第一次含水量过高"))
        out.collect((value._1,value._2,"含水量:"+nowCE +" 第二次含水量过高"))
      }
      firstCETarget.update(secondCETarget.value())
      secondCETarget.update(nowCE)
    }


    lastRecord.update(value)

  }
}

class OilAverageProcessFun extends KeyedProcessFunction[String,(String, String, String,String,String),(String, String, String,String,String)]{

  lazy val counter: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("counter",classOf[Long]))
  lazy val sumAE: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("sum-AE",classOf[Long]))
  lazy val currAE: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("curr-AE",classOf[Long]))

  lazy val sumAB: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("sum-AB",classOf[Long]))
  lazy val currAB: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("curr-AB",classOf[Long]))


  lazy val triggerTime: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("trigger-Time",classOf[Long]))



  override def processElement(value: (String, String, String, String, String), ctx: KeyedProcessFunction[String, (String, String, String, String, String), (String, String, String, String, String)]#Context, out: Collector[(String, String, String, String, String)]): Unit = {


  }
}