package mytest

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

import scala.collection.immutable
import scala.util.Random

/**
 * @author jiangfan
 * @date 2021/5/21 13:58
 */
object operatorState {
  def main(args: Array[String]): Unit = {
    val streamenv = StreamExecutionEnvironment.getExecutionEnvironment
    //  streamenv.setParallelism(1)
    streamenv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val input = streamenv.addSource(new operatorStateSource())
   input.process(new operatorStateProcess()).print()
   streamenv.execute()
  }
}

class operatorStateProcess extends ProcessFunction[(Long,Long),(Long,Long)]{
//  private var listState: ListState[Long] = _
  private var listState: ListState[Long] = getRuntimeContext.getListState(new ListStateDescriptor[Long]("test-long-operator-state", classOf[Long]))

//  override def open(parameters: Configuration): Unit = {
//    val stateDescriptor = new  ListStateDescriptor[Long]("test-long-operator-state", classOf[Long])
//    listState = getRuntimeContext.getListState(stateDescriptor)
//    println("qqqqqqqqqqqqqqq")
//  }

  override def processElement(value: (Long, Long), ctx: ProcessFunction[(Long, Long), (Long, Long)]#Context, out: Collector[(Long, Long)]): Unit = {
    listState.add(value._1)
    out.collect(value)
  }
}

class operatorStateSource() extends SourceFunction[(Long,Long)]{
  var running:Boolean=true
  //  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
  override def run(ctx: SourceFunction.SourceContext[(Long,Long)]): Unit = {
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
          val str = (t._1.toLong, t._2.toLong)
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