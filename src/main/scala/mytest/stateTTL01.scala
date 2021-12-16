package mytest

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{KeyedStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector
import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic

import scala.collection.immutable
import scala.util.Random

/**
 * @author jiangfan
 * @date 2021/5/18 18:14
 */

class testTTL extends RichFlatMapFunction[(String,Long,Int),Long]{
  private var sum: ValueState[Int] = _
  override def flatMap(value: (String, Long, Int), out: Collector[Long]): Unit = {
    val tmp: Int = sum.value() + value._3
    println(System.currentTimeMillis()+"==sum.value()======"+sum.value())
//    if(sum.value()<100){
//     sum.update(sum.value() + 1)
//      out.collect(tmp)
//    }
    if(value._2% 10 ==0){
      sum.update(tmp)
      out.collect(tmp)
    }
  }

  override def open(parameters: Configuration): Unit = {
    val ttlConfig = StateTtlConfig
      .newBuilder(Time.seconds(5))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build

    val stateDescriptor: ValueStateDescriptor[Int] = new ValueStateDescriptor[Int]("testintvalue", classOf[Int])
    stateDescriptor.enableTimeToLive(ttlConfig)
    sum = getRuntimeContext.getState(stateDescriptor)
    println("aaaaaaaaaaa")
  }
}

object stateTTL01 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(3)
    env.addSource(new SensorSource03()).keyBy(_._1).flatMap(new testTTL())
      .print()

    env.execute("test08KeyedState")
  }
}



class SensorSource03() extends SourceFunction[(String,Long,Int)]{
  var running:Boolean=true
  //  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
  override def run(ctx: SourceFunction.SourceContext[(String,Long,Int)]): Unit = {
    val rand = new Random()
    var curTemp: immutable.IndexedSeq[(String, Int)] = 1.to(1).map(
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
          val str = (t._1, curTime, t._2)
//                    println(str)
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
