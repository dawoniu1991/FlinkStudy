package TwoPhaseCommitTest

import com.alibaba.druid.pool.{DruidDataSource, DruidPooledConnection}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}
import org.apache.flink.streaming.api.scala.createTypeInformation
import utils.{DruidDSUtil, DruidDSUtil01, MysqlConnection}

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

/**
 * @author jiangfan
 * @date 2022/9/29 18:48
 */
class MysqlSink( config : ExecutionConfig) extends TwoPhaseCommitSinkFunction[(String,String,String),DruidPooledConnection,String](createTypeInformation[DruidPooledConnection].createSerializer(config),createTypeInformation[String].createSerializer(config)){
  override def invoke(transaction: DruidPooledConnection, value: (String, String, String), context: SinkFunction.Context[_]): Unit = {
    println(transaction+"invoke======"+value)
    val sql = "INSERT INTO t_book ( user_id ,username,ustatus) values (?,?,?)"
    val statement = transaction.prepareStatement(sql)
    statement.setString(1,"bbb")
    statement.setString(2,value._2)
    statement.setString(3,value._3)
    statement.execute()
    statement.close()
  }

//  private var druidDataSource:DruidDataSource = null
//  override def open(parameters: Configuration): Unit = {
//    val username = "root"
//    val password = "123456"
//    val driver = "com.mysql.jdbc.Driver"
//    val url = "jdbc:mysql://localhost:3306/mytest_db?useUnicode=true&characterEncoding=utf-8&useSSL=false"
//    druidDataSource = DruidDSUtil.createDataSource(username,password,driver,url)
//    println("open")
//  }

  override def beginTransaction(): DruidPooledConnection = {
    val url="jdbc:mysql://localhost:3306/mytest_db?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    val username="root"
    val password="123456"
    val driver = "com.mysql.jdbc.Driver"
//    val druidDataSource = DruidDSUtil.createDataSource(username,password,driver,url)
    val druidDataSource = DruidDSUtil01.getDataSource()
    druidDataSource.setDefaultAutoCommit(false)
    val connection: DruidPooledConnection =druidDataSource.getConnection
    println("beginTransaction")
//    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"beginTransaction connection===="+connection)
    connection
  }

  override def preCommit(transaction: DruidPooledConnection): Unit = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"start preCommit")
    Thread.sleep(2000)
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"end preCommit")
  }

  override def commit(transaction: DruidPooledConnection): Unit = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"start--commit")
    Thread.sleep(10000)
    transaction.commit()
    transaction.close()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"end--commit")
  }

//  override def abort(transaction: DruidPooledConnection): Unit = {
//    println("abort----start---")
//    transaction.close()
//    println("abort-----end--")
//  }
  override def abort(transaction: DruidPooledConnection): Unit = {
    println("abort")
  }
}
