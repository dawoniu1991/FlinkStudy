package sinktest

import java.util.Properties

import apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

//    val streamFile: DataStream[String] = env.readTextFile("src\\main\\resources\\sensor.txt")

//    val tool: ParameterTool = ParameterTool.fromArgs(args)
//    val path: String = tool.get("path")
//    val streamFile: DataStream[String] = env.readTextFile(path)

        val properties = new Properties()
        properties.setProperty("bootstrap.servers","localhost:9092")
        properties.setProperty("group.id","consumer-group01")
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset","latest")

        println("=================begin====================")
        val streamFile: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor02", new SimpleStringSchema(), properties))

    val dataStream: DataStream[String] = streamFile.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString

    })

    dataStream.addSink( new FlinkKafkaProducer011[String]("localhost:9092","sinkTest02",new SimpleStringSchema()))
    dataStream.print()

    env.execute("kafka sink test")

  }
}
