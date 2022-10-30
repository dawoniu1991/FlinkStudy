package TwoPhaseCommitTest

import com.alibaba.druid.pool.{DruidDataSourceFactory, DruidPooledConnection}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}
import org.apache.flink.streaming.api.scala.createTypeInformation
import scalaTest01.ScalaTest006.Student
import utils.{DruidDSUtil01, MysqlConnection}

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import java.sql.{Connection, DriverManager}
/**
 * @author jiangfan
 * @date 2022/9/30 10:48
 */
//class MysqlSinkDruid02 {
//
//}

class MysqlSinkDruid02(config : ExecutionConfig ) extends TwoPhaseCommitSinkFunction[(String,String,String),Connection,String](createTypeInformation[Connection].createSerializer(config),createTypeInformation[String].createSerializer(config)){
  override def invoke(transaction: Connection, value: (String, String,  String), context: SinkFunction.Context[_]): Unit = {
    println(transaction+"invoke======"+value)
    val sql = "INSERT INTO t_book ( user_id ,username,ustatus) values (?,?,?)"
    val statement = transaction.prepareStatement(sql)
    statement.setString(1,"ddd")
    statement.setString(2,value._2)
    statement.setString(3,value._3)
    statement.execute()
    statement.close()
  }

  override def beginTransaction(): Connection = {
    val url="jdbc:mysql://localhost:3306/mytest_db"
    val user="root"
    val password="123456"
//    Class.forName("com.mysql.jdbc.Driver")
//    val connection: Connection = DriverManager.getConnection(url, user, password)

    val props = new Properties
    props.put("driverClassName", "com.mysql.jdbc.Driver")
    props.put("url", "jdbc:mysql://localhost:3306/mytest_db?characterEncoding=utf8")
    props.put("username", "root")
    props.put("password", "123456")
    val druidDataSource = DruidDSUtil01.getDataSource()
    druidDataSource.setDefaultAutoCommit(false)
    val connection: DruidPooledConnection =druidDataSource.getConnection()
    connection.setAutoCommit(false)
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"beginTransaction connection===="+connection)
    if (connection.isInstanceOf[Connection]) {
      println("aaaaa")
      val s = connection.asInstanceOf[Connection]
      s
    }else{
      connection
    }
  }

  override def preCommit(transaction: Connection): Unit = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"start preCommit")
    Thread.sleep(3000)
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"end preCommit")
  }

  override def commit(transaction: Connection): Unit = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"start--commit")
    Thread.sleep(6000)
    transaction.commit()
    transaction.close()
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"end--commit")
  }

  override def abort(transaction: Connection): Unit = {
    println("abort----start---")
    transaction.close()
    println("abort-----end--")

  }
}
