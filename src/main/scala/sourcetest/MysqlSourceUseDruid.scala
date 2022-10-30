package sourcetest

import com.alibaba.druid.pool.{DruidDataSource, DruidPooledConnection}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import java.sql.{Connection, PreparedStatement}
import utils.DruidDSUtil
/**
 * @author jiangfan
 * @date 2022/10/9 15:15
 */
object MysqlSourceUseDruid {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source: DataStream[Student] = env.addSource(new SQL_source)
    source.print("res==")
    env.execute()
  }

  class SQL_source extends RichSourceFunction[Student]{
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

    override def run(sourceContext: SourceContext[Student]): Unit = {
      println("run==")
      while(true){
        println("read mysql")
        //获取连接
        connection = druidDataSource.getConnection()
        val sql = "select user_id , username ,ustatus from t_book"
        preparedStatement = connection.prepareStatement(sql)
        val queryRequest = preparedStatement.executeQuery()
        while (queryRequest.next()){
          val user_id = queryRequest.getString("user_id")
          val username = queryRequest.getString("username")
          val ustatus = queryRequest.getString("ustatus")
          val stu =  Student(user_id , username , ustatus )
          sourceContext.collect(stu)
        }
        //释放资源
        preparedStatement.close()
        //归还连接
        connection.close()
        Thread.sleep(10000)
      }
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
    override def cancel(): Unit = {}
  }


  case class Student(user_id:String , username:String , ustatus:String ){
    override def toString: String = {
      "user_id:"+user_id+"  username="+username+"   ustatus="+ustatus
    }
  }
}
