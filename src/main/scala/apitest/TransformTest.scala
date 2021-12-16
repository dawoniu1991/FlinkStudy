package apitest

import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val streamFile: DataStream[String] = env.readTextFile("src\\main\\resources\\sensor.txt")
    val value01: DataStream[SensorReading] = streamFile.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)

    })

//    val value02: KeyedStream[SensorReading, Tuple] = value01.keyBy(0)
//    val value03: DataStream[SensorReading] = value02.sum(2)

//    val value02: KeyedStream[SensorReading, String] = value01.keyBy(_.id)
//    val value03: DataStream[SensorReading] = value02.sum(2)

//val value02: KeyedStream[SensorReading, Tuple] = value01.keyBy("id")
//    val value03: DataStream[SensorReading] = value02.sum("temperature")
//val value02: KeyedStream[SensorReading, Tuple] = value01.keyBy("id")
//    val value03: DataStream[SensorReading] = value02.reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature+10))

//    value03.print()


//    val value02: SplitStream[SensorReading] = value01.split(data => {
//      if (data.temperature > 30) Seq("high") else Seq("low")
//    })
//
//    val high: DataStream[SensorReading] = value02.select("high")
//    val low: DataStream[SensorReading] = value02.select("low")
//    val all: DataStream[SensorReading] = value02.select("high", "low")
//coMap合流操作
//    val warning: DataStream[(String, Double)] = high.map(data => (data.id, data.temperature))
//    val connectedStream: ConnectedStreams[(String, Double), SensorReading] = warning.connect(low)
//    val coMapDataStream: DataStream[Product] = connectedStream.map(x => (x._1, x._2, "warning"), y => (y.id, "healthy"))
//    coMapDataStream.print()

// high.print("high")
// low.print("low")
// all.print("all")
    //union合流
//val value03: DataStream[SensorReading] = high.union(low)
//value03.print()

//    value01.filter(new MyFilter()).print()
    value01.map(new MyMapper()).print()
    env.execute("transform test")
  }
}

class MyFilter() extends FilterFunction[SensorReading]{
  override def filter(t: SensorReading): Boolean = {
    t.id.startsWith("sensor_1")
  }
}

class MyMapper() extends RichMapFunction[SensorReading,String]{
  override def map(in: SensorReading): String = {
    "aaaakkkk"
  }

  override def open(parameters: Configuration): Unit = super.open(parameters)
}
