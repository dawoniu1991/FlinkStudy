package sinktest


import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.immutable
import scala.util.Random


object MysqlSinkUseJdbc {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 读取数据
    //    val filePath = "src\\main\\resources\\sensor.txt"
    //    val inputStream: DataStream[String] = env.readTextFile(filePath)
    val inputStream = env.addSource( new SensorSource() )

    // 写入mySQL
    inputStream.addSink( new MyJdbcSink() )
    env.execute("jdbc sink job")
  }

  case class SensorReading(user_id:String,username:String,ustatus:String)

  class SensorSource() extends SourceFunction[SensorReading]{
    var running:Boolean=true
    override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
      val rand = new Random()
      var curTemp: immutable.IndexedSeq[(String, Double)] = 1.to(5).map(
        i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
      )

      println("00000000000000000000000")
      while(running){
        curTemp= curTemp.map(
          t => ( t._1,t._2+rand.nextGaussian())
        )

        val curTime: Long = System.currentTimeMillis()
        curTemp.foreach(
          t=>ctx.collect(SensorReading(t._1,curTime.toString,t._2.toString))
        )
        println("=================================================")
        Thread.sleep(15000)
      }
    }

    override def cancel(): Unit = {
      running=false
    }
  }

  // 自定义实现SinkFunction
  class MyJdbcSink() extends RichSinkFunction[SensorReading]{
    // 定义sql连接、预编译语句
    var conn: Connection = _
    var insertStmt: PreparedStatement = _
    var updateStmt: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      // 创建连接，并实现预编译语句
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/mytest_db", "root", "123456")
      updateStmt = conn.prepareStatement("update t_book set username = ? and ustatus = ? where user_id = ?")
      insertStmt = conn.prepareStatement("insert into t_book (user_id , username ,ustatus ) values (?, ?, ?)")
    }

    override def invoke(value: SensorReading, context: _root_.org.apache.flink.streaming.api.functions.sink.SinkFunction.Context[_]): Unit = {
      // 直接执行更新语句，如果没有更新就插入
      updateStmt.setString(1, value.username)
      updateStmt.setString(2, value.ustatus)
      updateStmt.setString(3, value.user_id)
      updateStmt.execute()
      if( updateStmt.getUpdateCount == 0 ){
        println("已经存在，更新数据")
        insertStmt.setString(1, value.user_id)
        insertStmt.setString(2, value.username)
        insertStmt.setString(3, value.ustatus)
        insertStmt.execute()
      }
    }

    override def close(): Unit = {
      println("close")
      insertStmt.close()
      updateStmt.close()
      conn.close()
    }
  }
}

