package mytest

/**
 * @author jiangfan
 * @date 2021/5/20 16:45
 */

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.immutable
import scala.util.Random

object StreamingDemoWithMyPartition {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val text = env.addSource(new SensorSource06())
    val tupdata = text.map(line=>{Tuple1(line._2)})
    val partdata = tupdata.partitionCustom(new MyPartition,0)
    val result = partdata.map(line => {
      println("当前线程id" + Thread.currentThread().getId + ",value" + line)
      line._1
    })
    result.print()

    env.execute("StreamingDemoWithMyPartition")
  }

  class MyPartition extends Partitioner[Long]{
    override def partition(key: Long, numpartition: Int): Int = {
      System.out.println("总的分区数"+numpartition)
      if (key%2==0){
        0
      }else{
        1
      }
    }
  }
}


class SensorSource06() extends SourceFunction[(String,Long,Int)]{
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