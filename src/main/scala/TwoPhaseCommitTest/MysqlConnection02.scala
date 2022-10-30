package TwoPhaseCommitTest

import com.alibaba.druid.pool.{DruidDataSource, DruidDataSourceFactory, DruidPooledConnection}

import java.sql.Connection
import java.util.Properties
import javax.sql.DataSource

/**
 * @author jiangfan
 * @date 2022/9/30 15:47
 */
object MysqlConnection02 {
   var  source:DataSource=null
  var count=0
  def initDruidSource(user:String,password:String,url:String): Unit ={
    if(source!=null){
      println("exist cunzaile====")
      return
    }
    val props = new Properties
    props.put("driverClassName", "com.mysql.jdbc.Driver")
    props.put("url", "jdbc:mysql://localhost:3306/mytest_db?characterEncoding=utf8")
    props.put("username", "root")
    props.put("password", "123456")
    source = DruidDataSourceFactory.createDataSource(props)

    println("init datasource02")
  }



}
