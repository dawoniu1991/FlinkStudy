package sinktest

import com.alibaba.druid.pool.DruidDataSource
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import utils.DruidDSUtil

import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.util.Random

/**
 * @author jiangfan
 * @date 2022/10/9 16:47
 */
object MysqlSinkUseDruid {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 读取数据
    //    val filePath = "src\\main\\resources\\sensor.txt"
    //    val inputStream: DataStream[String] = env.readTextFile(filePath)
    //读取自定义数据源
    val inputStream = env.addSource( new SensorSource() )

    // 写入mySQL
    inputStream.print("aa=")
    inputStream.addSink( new MyDruidSink() )
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
  class MyDruidSink() extends RichSinkFunction[SensorReading] {
    private var connection: Connection = null
    private var preparedStatement: PreparedStatement = null
    private var druidDataSource:DruidDataSource = null
    override def open(parameters: Configuration): Unit = {
      val username = "root"
      val password = "123456"
      val driver = "com.mysql.jdbc.Driver"
      val url = "jdbc:mysql://localhost:3306/mytest_db?useUnicode=true&characterEncoding=utf-8&useSSL=false"
      druidDataSource = DruidDSUtil.createDataSource(username,password,driver,url)
    }

    override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
        println("write mysql"+getRuntimeContext.getIndexOfThisSubtask)
        //获取连接
        connection = druidDataSource.getConnection()
        val sql = "insert into t_book (user_id , username ,ustatus ) values (?, ?, ?)"
        preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, value.user_id)
        preparedStatement.setString(2, value.username)
        preparedStatement.setString(3, value.ustatus)
        preparedStatement.execute()
        //释放资源
        preparedStatement.close()
        //归还连接
        connection.close()
    }

    override def close(): Unit = {
      println("close=====")
      if(connection != null){
        connection.close()
      }
      if(preparedStatement != null){
        preparedStatement.close()
      }
    }
  }
}
