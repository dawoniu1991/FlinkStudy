package broadcast

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import redis.clients.jedis.JedisCluster
import utils.{LoggerPrint, RedisUtil}

import com.alibaba.fastjson.JSON
import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util
import java.util.Properties
import scala.collection.JavaConversions._
/**
 * @author jiangfan
 * @date 2022/10/10 17:43
 */
class RedisBroadcastSource(config:Properties) extends RichSourceFunction[util.Map[String, String]] {
  var redisCluster:JedisCluster=null
  var loadOnceFromFile=0
  var rulesFile="rules/init_rule.txt"
  override def open(parameters: Configuration): Unit = {
    val redisHosts=config.getProperty("redisHosts")
    val redisPassword=config.getProperty("redisPassword")
    redisCluster=RedisUtil.getRedisConnect(redisHosts,redisPassword )
  }

  override def run(ctx: SourceFunction.SourceContext[util.Map[String, String]]): Unit = {
     while(true){
       var filterRule: util.Map[String, String] = new util.HashMap[String, String]()
       if(config.getProperty("initLoadFromFile").equals("true")&& loadOnceFromFile==0){
         filterRule=getRulesFromFile(rulesFile)
         loadOnceFromFile=1
         LoggerPrint.info("load from file")
       }else{
         filterRule=redisCluster.hgetAll("rediskey")
         if(filterRule.size()==0){
           filterRule=getRulesFromFile(rulesFile)
           LoggerPrint.info("redis配置为空，从文件加载配置规则")
         }
         LoggerPrint.info("load from redis")
       }
       LoggerPrint.info(filterRule)
       if(filterRule!=null){
         ctx.collect(filterRule)
       }
       Thread.sleep(1000*60)
     }
  }

  override def cancel(): Unit = {
    println("redis read error")
  }

  def getRulesFromFile(input:String)={
    val in: InputStream = this.getClass.getClassLoader.getResourceAsStream(input)
    val reader: BufferedReader = new BufferedReader(new InputStreamReader(in))
    var text=""
    var line=reader.readLine()
    while(line!=null){
      text=text+line.trim
      line=reader.readLine()
    }
    val jsonObject = JSON.parseObject(text)
    val rulesMap: util.Map[String, String] = new util.HashMap[String, String]()
    for(item <- jsonObject){
      rulesMap.put(item._1,item._2.toString)
    }
    rulesMap
  }
}
