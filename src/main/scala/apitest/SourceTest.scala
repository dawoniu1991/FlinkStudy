package apitest

import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.collection.immutable
import scala.util.Random


case class SensorReading(id:String,timestamp:Long,temperature:Double)
object SourceTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//    val stream1: DataStream[SensorReading] = env.fromCollection(List(
//      SensorReading("sensor_1", 1547718199, 35.80018327300259),
//      SensorReading("sensor_6", 1547718201, 15.80018327300259),
//      SensorReading("sensor_7", 1547718202, 6.72),
//      SensorReading("sensor_10", 1547718205, 38.1018327300259)
//    ))

//    val value: DataStream[Any] = env.fromElements(1, 2.5, "sssaa")
//    env.fromElements(1, 2.5, "sssaa").print()
//    val stream2: DataStream[String] = env.readTextFile("F:\\myflinkstudy\\flinktest\\src\\main\\resources\\sensor.txt")


//    stream1.print().setParallelism(1)
//    stream2.print("stream02").setParallelism(8)


    val properties = new Properties()
    properties.setProperty("bootstrap.servers","localhost:9092")
    properties.setProperty("group.id","consumer-group01")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","latest")

    println("=================begin=================")

    val stream3: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("first001", new SimpleStringSchema(), properties))
    stream3.print("stream04").setParallelism(1)


//    val stream4: DataStream[SensorReading] = env.addSource(new SensorSource())
//    stream4.print("stream04").setParallelism(1)

    env.execute("source test")
  }
}

class SensorSource() extends SourceFunction[SensorReading]{
  var running:Boolean=true
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()
    var curTemp: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(
      i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
    )

    println("00000000000000000000000")
    while(running){
      curTemp= curTemp.map(
        t => ( t._1,t._2+rand.nextGaussian())
      )

      val curTime: Long = System.currentTimeMillis()
      curTemp.foreach(
        t=>ctx.collect(SensorReading(t._1,curTime,t._2))
      )
      println("=================================================")
      Thread.sleep(1000)
    }

  }

  override def cancel(): Unit = {
    running=false
  }
}
