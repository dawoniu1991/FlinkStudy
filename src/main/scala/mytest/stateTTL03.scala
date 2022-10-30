package mytest

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
//
import scala.collection.immutable
import scala.util.Random

/**
 * @author jiangfan
 * @date 2021/5/19 9:49
 */
class testkeyedprocess extends KeyedProcessFunction[String,(String,Long,Int),(String,Long,Int)]{
  private var valuestate: ValueState[Long] = _
  override def processElement(value: (String, Long, Int), ctx: KeyedProcessFunction[String, (String, Long, Int), (String, Long, Int)]#Context, out: Collector[(String, Long, Int)]): Unit = {
    val tmp = valuestate.value()
//    println(ctx.getCurrentKey)
    println(ctx.getCurrentKey+"end~~~~~~~~~"+tmp)
    if(tmp==0){
      valuestate.update(tmp+1)
      println("this is initial key========================================="+ctx.getCurrentKey)
      out.collect(value)
    }else if(tmp<10){
        valuestate.update(tmp+1)
        //        out.collect(value)
        out.collect((value._1,value._2,tmp.toInt))
      }else{
      ctx.output(new OutputTag[(String, Long, Int)]("black-list"),(value._1,value._2,tmp.toInt))
    }

  }

  override def open(parameters: Configuration): Unit = {
    val ttlConfig = StateTtlConfig
      .newBuilder(Time.seconds(10))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build

    val stateDescriptor = new  ValueStateDescriptor[Long]("test-long-state", classOf[Long])
    stateDescriptor.enableTimeToLive(ttlConfig)
    valuestate = getRuntimeContext.getState(stateDescriptor)
    println("wwwwwwwwwwwwww")
  }
}

//val stateDescriptor = new MapStateDescriptor[String, Long]("pageViewCount-map", classOf[String], classOf[Long])


object stateTTL03 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(2)
//    val result: DataStream[(String, Long, Int)] = env.addSource(new SensorSource04()).keyBy(_._1).process(new testkeyedprocess())
    val result: DataStream[(String, Long, Int)] = env.addSource(new SensorSource05()).keyBy(_._1).process(new testkeyedprocess())
    result.print("result====")
    result.getSideOutput(new OutputTag[(String, Long,Int)]("black-list")).print("side===================")
    env.execute("test-Keyed-State")
  }
}


class SensorSource05() extends SourceFunction[(String,Long,Int)]{
  var running:Boolean=true
  //  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
  override def run(ctx: SourceFunction.SourceContext[(String,Long,Int)]): Unit = {
    val rand = new Random()
    var curTemp: immutable.IndexedSeq[(String, Int)] = 1.to(3).map(
      i => ("sensor_" + i, 60 + rand.nextInt(10) * 20)
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
//          val str = (t._1, curTime, t._2)
          val str = ("sen_"+rand.nextInt(5).toString, curTime, t._2)
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
