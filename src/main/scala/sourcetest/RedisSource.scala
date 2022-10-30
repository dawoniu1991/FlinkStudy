package sourcetest

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import redis.clients.jedis.JedisCluster
import sourcetest.MysqlSourceUseDruid.{SQL_source, Student}
import utils.RedisUtil

import java.util

/**
 * @author jiangfan
 * @date 2022/10/9 17:45
 */
object RedisSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source: DataStreamSource[util.Map[String, String]] = env.addSource(new MyRedisSource)
    source.print("res==")
    env.execute()
  }

  class MyRedisSource extends RichSourceFunction[util.Map[String,String]]{
    var redisCluster:JedisCluster=null
    override def open(parameters: Configuration): Unit = {
      val redisHosts=""
      val redisPassword=""
      redisCluster=RedisUtil.getRedisConnect(redisHosts,redisPassword )
    }
    override def run(ctx: SourceFunction.SourceContext[util.Map[String, String]]): Unit = {
      val data: util.Map[String, String] = redisCluster.hgetAll("mykey")
      if(data!=null && data.size()!=0){
        ctx.collect(data)
      }
    }

    override def cancel(): Unit = {
    }
  }

}
