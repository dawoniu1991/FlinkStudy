package mytest

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
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
class testflatmap extends KeyedProcessFunction[String,(String,Long,Int),(String,Long,Int)]{
  private var mapstate: MapState[String, Long] = _
  override def processElement(value: (String, Long, Int), ctx: KeyedProcessFunction[String, (String, Long, Int), (String, Long, Int)]#Context, out: Collector[(String, Long, Int)]): Unit = {
//    val key1 = ctx.getCurrentKey
//    println("key1======"+key1)
//    val value1 = mapstate.keys().iterator()
//    if(value1.hasNext) {
//      println("key========="+value1.next()+"cur=="+ctx.getCurrentKey)
//    }

    val allPageViewCounts: ListBuffer[(String, Long)] = ListBuffer()
    val iter = mapstate.entries().iterator()
    while(iter.hasNext){
      val entry = iter.next()
      allPageViewCounts += ((entry.getKey, entry.getValue))
    }

    println("end~~~~~~~~~"+allPageViewCounts.size)
    val key = value._1
    if(!mapstate.contains(key)){
      mapstate.put(key,1)
      println("this is 1 initial=============================================")
      out.collect(value)
    }else{
      val historyCount: Long = mapstate.get(key)
      if(historyCount<10){
        mapstate.put(key,historyCount + 1)
//        out.collect(value)
        out.collect((value._1,value._2,historyCount.toInt))
      }
    }
//    if(!allPageViewCounts.isEmpty) {
//      ctx.output(new OutputTag[(String, Long)]("black-list"),
//        (allPageViewCounts(0)._1, allPageViewCounts(0)._2))
//    }
  }

  override def open(parameters: Configuration): Unit = {
    val ttlConfig = StateTtlConfig
      .newBuilder(Time.seconds(10))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build
    val stateDescriptor = new MapStateDescriptor[String, Long]("pageViewCount-map", classOf[String], classOf[Long])
    stateDescriptor.enableTimeToLive(ttlConfig)
    mapstate = getRuntimeContext.getMapState(stateDescriptor)
    println("qqqqqqqqqqqqqqqqqqqq")
  }
}

//val stateDescriptor = new MapStateDescriptor[String, Long]("pageViewCount-map", classOf[String], classOf[Long])


object stateTTL02 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(2)
    val result: DataStream[(String, Long, Int)] = env.addSource(new SensorSource04()).keyBy(_._1).process(new testflatmap())
    result.print("result====")
//    result.getSideOutput(new OutputTag[(String, Long)]("black-list")).print("side==")
    env.execute("test KeyedState")
  }
}


class SensorSource04() extends SourceFunction[(String,Long,Int)]{
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
          val str = (t._1, curTime, t._2)
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
