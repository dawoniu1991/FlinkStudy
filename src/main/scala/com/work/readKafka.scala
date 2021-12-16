package com.work

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.contrib.streaming.state.{PredefinedOptions, RocksDBStateBackend}
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties
import java.util.concurrent.TimeUnit

/**
 * @author jiangfan
 * @date 2021/12/8 13:55
 */
object readKafka {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setMaxParallelism(1)
    env.enableCheckpointing(300000)
      .setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.getConfig.setAutoWatermarkInterval(60000)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(300000)
    env.getCheckpointConfig.setCheckpointTimeout(1000L * 60L * 60L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    val checkpointDir="s3://mob-emr-test/fan.jiang/mywork/flink/jiangfan_test_read/read_from_kafka/checkpoint"
    val backend = new RocksDBStateBackend(checkpointDir, true)
    //    backend.enableTtlCompactionFilter()
    backend.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.setStateBackend(backend.asInstanceOf[StateBackend])
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
      org.apache.flink.api.common.time.Time.of(10, TimeUnit.MINUTES),
      org.apache.flink.api.common.time.Time.of(15, TimeUnit.SECONDS)))

    /**
     * kafka configuration option
     */
    val BOOTSTRAP_SERVERS = "bootstrap.servers"
    val GROUP_ID = "group.id"
    val AUTO_OFFSET_RESET = "auto.offset.reset"
    val props = new Properties()

    val TOPIC_NAME = "adn-tracking_v3_fluentd_click-log_txt-0"
    props.setProperty(BOOTSTRAP_SERVERS, "")
    props.setProperty(GROUP_ID, "jiangfan_get_sample_kafka_data20211208")
    props.setProperty(AUTO_OFFSET_RESET, "latest")

    val dataSource =
      env.addSource[String](new FlinkKafkaConsumer011[String](TOPIC_NAME, new SimpleStringSchema(), props)
        .setCommitOffsetsOnCheckpoints(true))
        .setParallelism(1).name("source-test").uid("source-test")

    val sinkPath="s3://mob-emr-test/fan.jiang/mywork/flink/jiangfan_test_read/read_from_kafka/result"
    val sinkFinal = StreamingFileSink.forRowFormat(new Path(sinkPath), new SimpleStringEncoder[String]("UTF-8"))
      .withBucketAssigner(new DateTimeBucketAssigner[String]("yyyy-MM-dd/HH/mm")) //设置时间的桶
      .withRollingPolicy(OnCheckpointRollingPolicy.build()) //以checkpoint为触发滚动的时机
      .build()

    dataSource.addSink(sinkFinal).name("sink-final").uid("sink-final")
    env.execute("jiangfan_test_read_kafka")
  }
}
