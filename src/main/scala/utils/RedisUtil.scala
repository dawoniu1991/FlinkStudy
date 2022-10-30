package utils

/**
 * @author jiangfan
 * @date 2022/10/9 17:24
 */
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

import java.util
import scala.collection.JavaConversions._

object RedisUtil {
  def main(args: Array[String]): Unit = {
    println("aaa")
  }
   def getRedisConnect(redisHosts:String,redisPassword:String):JedisCluster={
     val strings = redisHosts.split(",")
     val hashSet = new util.HashSet[HostAndPort]()
     for(redisHost<-strings){
       val stringsArr = redisHost.split(":")
       val ip = stringsArr.apply(0)
       val port = stringsArr.apply(1)
       hashSet.add(new HostAndPort(ip,port.toInt))
     }
     val config = new JedisPoolConfig
     config.setMaxTotal(100)
     config.setMaxIdle(-1)
     config.setMaxWaitMillis(3000)
     config.setTestOnBorrow(true)
     config.setTestOnReturn(true)
     val cluster = new JedisCluster(hashSet, 15000, 10000, 10, redisPassword, config)
     cluster
   }
}
