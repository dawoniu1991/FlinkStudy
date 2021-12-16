package apitest

/**
 * @author jiangfan
 * @date 2021/12/14 21:10
 */


import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.collection.immutable
import scala.util.Random


//創建topic
//bin/kafka-topics.sh --zookeeper localhost:2181 --create  --replication-factor 1 --partitions 2 --topic first001
//
//生產消息
//bin/kafka-console-producer.sh --broker-list localhost:9092 --topic first001
//消費消息
//bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first001
//列出所有消費group
//bin/kafka-consumer-groups.sh  --bootstrap-server localhost:9092 --list
//
//列出消費group  'consumer-group111' 下的消費信息
//bin/kafka-consumer-groups.sh  --bootstrap-server localhost:9092  --group  consumer-group111   --describe


//bin/kafka-console-consumer.sh  --bootstrap-server localhost:9092 --topic first001

//解压flink-1.10.1-bin-scala_2.11.tgz   然后直接执行  bin/start-cluster.sh  启动集群
//运行作业  ./bin/flink run  -c apitest.KafkaSource -p 1 /mnt/f/myflinkstudy/FlinkStudy/target/FlinkStudy-1.0-SNAPSHOT-jar-with-dependencies.jar  --host localhost --port 7777
object KafkaSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","localhost:9092")
    properties.setProperty("group.id","consumer-group111")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
//    1.earliest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
//    2.latest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
    properties.setProperty("auto.offset.reset","latest")
//    properties.setProperty("auto.offset.reset","earliest")

    println("=================begin=================")

    val stream3: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("first001", new SimpleStringSchema(), properties))
    stream3.print("stream01").setParallelism(1)

    env.execute("source test")
  }
}

