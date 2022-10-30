package sourcetest

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * @author jiangfan
 * @date 2022/10/9 14:36
 */
object MysqlSourceUseJdbc {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source: DataStream[Student] = env.addSource(new SQL_source)
    source.print("res==")
    env.execute()
  }


  class SQL_source extends RichSourceFunction[Student]{
    private var connection: Connection = null
    private var ps: PreparedStatement = null

    override def open(parameters: Configuration): Unit = {
      val driver = "com.mysql.jdbc.Driver"
      val url = "jdbc:mysql://localhost:3306/mytest_db"
      val username = "root"
      val password = "123456"
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      val sql = "select user_id , username ,ustatus from t_book"
      ps = connection.prepareStatement(sql)
    }

    override def close(): Unit = {
      println("close=====")
      if(connection != null){
        connection.close()
      }
      if(ps != null){
        ps.close()
      }
    }


    override def run(sourceContext: SourceContext[Student]): Unit = {
      println("run==")
      while(true){
        println("read mysql")
        val queryRequest = ps.executeQuery()
        while (queryRequest.next()){
          val user_id = queryRequest.getString("user_id")
          val username = queryRequest.getString("username")
          val ustatus = queryRequest.getString("ustatus")
          val stu =  Student(user_id , username , ustatus )
          sourceContext.collect(stu)
        }
        Thread.sleep(10000)
      }
    }
    override def cancel(): Unit = {}
  }


  case class Student(user_id:String , username:String , ustatus:String ){
    override def toString: String = {
      "user_id:"+user_id+"  username:"+username+"   ustatus:"+ustatus
    }
  }
}
