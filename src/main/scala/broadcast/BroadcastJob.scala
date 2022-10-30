package broadcast

import com.alibaba.fastjson.JSONObject
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.scala._
import utils.{KafkaUtil, ScalaUtil}

import java.util

/**
 * @author jiangfan
 * @date 2022/10/10 16:42
 */
object BroadcastJob {
  def main(args: Array[String]): Unit = {
    val config = ScalaUtil.loadConfig()
    println(config)
    val logLevel = config.getProperty("logLevel").toInt
    val jobName = config.getProperty("job.name")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val mapstate: MapStateDescriptor[String, util.Map[String, String]] = new MapStateDescriptor[String, util.Map[String, String]]("mapstate", classOf[String], classOf[util.Map[String, String]])
    val redisData: DataStream[util.Map[String, String]] =env.addSource(new RedisBroadcastSource(config))
    val inputTopic = config.getProperty("topic.input")
    val inputKafka = KafkaUtil.readKafka(inputTopic, config)
    val kafkaData: DataStream[(String, JSONObject)] =env.addSource(inputKafka).flatMap(new KafkaParseFlatMapFun(config))
    val output=kafkaData.keyBy(_._1).connect(redisData.broadcast(mapstate)).process(new BroadcastFun(mapstate))
    output.print()
    env.execute(jobName)
  }
}
