package com.work

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector
import org.apache.flink.contrib.streaming.state.{PredefinedOptions, RocksDBStateBackend}

/**
 * @author jiangfan
 * @date 2022/6/19 13:24
 */

object Job01 {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)   // 可以在本地cmd，然后bash  nc -lk 7777 开启端口发送数据进行访问
    val inputStream: DataStream[String] = env.readTextFile("src\\main\\resources\\job01.csv")
//    T1,2020-01-30 19:30:40,27.65;
//    Q1,2020-01-30 19:30:40,AB:38.9,AE:221323,CE:0.00001;


    val filterData = inputStream.process(new filterTag())

    val tempData = filterData.getSideOutput(new OutputTag[String]("temp"))
    val oilData = filterData.getSideOutput(new OutputTag[String]("oil"))

    val dataStream: DataStream[(String, String, Double)] = tempData
      .map(MyUtil.extract(_))
    dataStream.keyBy(_._1).process(new tempProcessFun()).print("res=")
    env.execute("job01=====")
//    maxStateSize
//    MemoryStateBackend()
//    RocksDBStateBackend()
  }
}

class filterTag() extends ProcessFunction[String,String]{
  lazy val tempOutput:OutputTag[String]=new OutputTag[String]("temp")
  lazy val oilOutput:OutputTag[String]=new OutputTag[String]("oil")
  override def processElement(value: String, ctx: ProcessFunction[String,String]#Context, out: Collector[String]): Unit = {
    if(value.startsWith("T")){
      ctx.output(tempOutput,value)
    }else if(value.startsWith("Q")){
      ctx.output(oilOutput,value)
    }
  }
}


class tempProcessFun() extends KeyedProcessFunction[String,(String, String, Double),(String, String, Double,String)]{

  lazy val averageTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("average-Temp",classOf[Double]))
  lazy val counter: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("counter",classOf[Long]))
  lazy val sumTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("sum-Temp",classOf[Double]))

  override def open(parameters: Configuration): Unit = {

  }

  override def processElement(value: (String, String, Double), ctx: KeyedProcessFunction[String, (String, String, Double), (String, String, Double, String)]#Context, out: Collector[(String, String, Double, String)]): Unit = {

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



  if( averageTemp.value()!=null ) {
    if( value._3-averageTemp.value() >5 ) {
     out.collect((value._1,value._2,value._3,"温度过高"))
    }else if( averageTemp.value()-value._3 >5 ){
      out.collect((value._1,value._2,value._3,"温度过低"))
    }
  }

    averageTemp.update(sumTemp.value()/counter.value())

  }
}
