package TwoPhaseCommitTest

import com.alibaba.druid.pool.DruidPooledConnection
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}
import org.apache.flink.streaming.api.scala.createTypeInformation
import utils.{DruidDSUtil, DruidDSUtil01, MysqlConnection}

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author jiangfan
 * @date 2022/9/30 10:48
 */
//class MysqlSinkDruid01 {
//
//}

class MysqlSinkDruid01( config : ExecutionConfig) extends TwoPhaseCommitSinkFunction[(String,String,String),ConnectionState,String](createTypeInformation[ConnectionState].createSerializer(config),createTypeInformation[String].createSerializer(config)){
  override def invoke(transaction: ConnectionState, value: (String,  String, String), context: SinkFunction.Context[_]): Unit = {
    println(transaction.connection+"invoke======"+value)
    val sql = "INSERT INTO t_book ( user_id ,username,ustatus) values (?,?,?)"
    val statement = transaction.connection.prepareStatement(sql)
    statement.setString(1,"ccc")
    statement.setString(2,value._2)
    statement.setString(3,value._3)
    statement.execute()
    statement.close()
  }

  override def beginTransaction(): ConnectionState = {
    val url="jdbc:mysql://localhost:3306/mytest_db"
    val username="root"
    val password="123456"
    val driver = "com.mysql.jdbc.Driver"
    val druidDataSource = DruidDSUtil01.getDataSource()
    druidDataSource.setDefaultAutoCommit(false)
    val connection: DruidPooledConnection =druidDataSource.getConnection

    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"beginTransaction ===="+connection)
    new ConnectionState(connection)
  }

  override def preCommit(transaction: ConnectionState): Unit = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"start preCommit"+transaction.connection)
    Thread.sleep(3000)
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"end preCommit"+transaction.connection)
  }

  override def commit(transaction: ConnectionState): Unit = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"start--commit"+transaction.connection)
    Thread.sleep(3000)
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"start--11commit"+transaction.connection)
    transaction.connection.commit()
    transaction.connection.close()
    println(transaction.connection==null)
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"end--commit"+transaction.connection)
  }

  override def abort(transaction: ConnectionState): Unit = {
    println("abort----start---")
    transaction.connection.close()
    println("abort-----end--")
  }
}
class ConnectionState( @transient val  connection: Connection ) {

}