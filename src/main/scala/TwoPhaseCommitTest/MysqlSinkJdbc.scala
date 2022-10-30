package TwoPhaseCommitTest

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}
import org.apache.flink.streaming.api.scala.createTypeInformation

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author jiangfan
 * @date 2022/9/29 19:48
 */

class MysqlSinkJdbc(config : ExecutionConfig ) extends TwoPhaseCommitSinkFunction[(String,String,String),Connection,String](createTypeInformation[Connection].createSerializer(config),createTypeInformation[String].createSerializer(config)){
  override def invoke(transaction: Connection, value: (String, String, String), context: SinkFunction.Context[_]): Unit = {
    println(transaction+"invoke======"+value)
    val sql = "INSERT INTO t_book ( user_id ,username,ustatus) values (?,?,?)"
    val statement = transaction.prepareStatement(sql)
    statement.setString(1,"aa")
    statement.setString(2,value._3)
    statement.setInt(3,666)
    statement.execute()
    statement.close()
  }

  override def beginTransaction(): Connection = {
    val url="jdbc:mysql://localhost:3306/mytest_db"
    val user="root"
    val password="123456"
    Class.forName("com.mysql.jdbc.Driver")
    val connection: Connection = DriverManager.getConnection(url, user, password)
    connection.setAutoCommit(false)
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"beginTransaction connection===="+connection)
    connection
  }

  override def preCommit(transaction: Connection): Unit = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"start preCommit")
    Thread.sleep(2000)
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"end preCommit")
  }

  override def commit(transaction: Connection): Unit = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"start--commit")
    Thread.sleep(10000)
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
