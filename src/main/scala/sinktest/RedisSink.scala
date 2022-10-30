package sinktest

import com.alibaba.druid.pool.DruidDataSource
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import redis.clients.jedis.JedisCluster
import utils.{DruidDSUtil, RedisUtil}

import java.sql.{Connection, PreparedStatement}
import scala.util.Random

/**
 * @author jiangfan
 * @date 2022/10/10 14:49
 */
object RedisSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 读取数据
    //    val filePath = "src\\main\\resources\\sensor.txt"
    //    val inputStream: DataStream[String] = env.readTextFile(filePath)
    //读取自定义数据源
    val inputStream = env.addSource( new SensorSource() )
    // 写入redis
    inputStream.print("aa=")
    inputStream.addSink( new MyRedisSink() )
    env.execute("jdbc sink job")
  }
  case class SensorReading(user_id:String,username:String,ustatus:String)

  class SensorSource() extends SourceFunction[SensorReading]{
    var running:Boolean=true
    override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
      val rand = new Random()
      println("00000000000000000000000")
      while(running){
        val curTime: Long = System.currentTimeMillis()
        val data=rand.nextInt(100).toString
        ctx.collect(SensorReading(data,curTime.toString,"sensor"+data))
        println("=================================================")
        Thread.sleep(15000)
      }
    }

    override def cancel(): Unit = {
      running=false
    }
  }

  // 自定义实现SinkFunction
  class MyRedisSink() extends RichSinkFunction[SensorReading] {
    var redisCluster:JedisCluster=null
    override def open(parameters: Configuration): Unit = {
      val redisHosts=""
      val redisPassword=""
      redisCluster=RedisUtil.getRedisConnect(redisHosts,redisPassword )
    }

    override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
      val key=""
      val value=""
      redisCluster.sadd(key,value)
    }

    override def close(): Unit = {
      println("close=====")
    }
  }
}
