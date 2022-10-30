package utils

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

/**
 * @author jiangfan
 * @date 2022/10/10 17:29
 */
object KafkaUtil {
   def readKafka(topic:String,config:Properties)={
     val props = new Properties()
     props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,config.getProperty("bootstrap.servers"))
     props.put(ConsumerConfig.GROUP_ID_CONFIG,config.getProperty("group.id"))
     props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,config.getProperty("auto.offset.reset"))
     new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),props)
   }
}
