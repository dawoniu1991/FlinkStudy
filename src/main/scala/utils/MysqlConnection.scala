package utils

import com.alibaba.druid.pool.{DruidDataSource, DruidDataSourceFactory, DruidPooledConnection}

import java.sql.Connection
import java.util.Properties
import javax.sql.DataSource

/**
 * @author jiangfan
 * @date 2022/9/29 10:14
 */
//object MysqlConnection extends Serializable {
object MysqlConnection {
  private var source:DruidDataSource=null
  var count=0
  def initDruidSource(user:String,password:String,driver:String,url:String): Unit ={
     if(source!=null){
       println("yijing cunzaile====")
       return
     }
    count+=1
    source=new DruidDataSource()
    source.setDriverClassName(driver)
    source.setUsername(user)
    source.setPassword(password)
    source.setUrl(url)
    source.setMaxActive(5)
    source.setMinIdle(2)

    source.setRemoveAbandoned(true)
    source.setRemoveAbandonedTimeout(120)
    source.setTimeBetweenEvictionRunsMillis(60000)
    source.setMinEvictableIdleTimeMillis(300000)
    source.setTestWhileIdle(true)
    source.setTestOnBorrow(true)
    source.setDefaultAutoCommit(false)
    println("init mysqldatasource")
  }

 def init2()= {
    val props = new Properties
   props.put("driverClassName", "com.mysql.jdbc.Driver")
   props.put("url", "jdbc:mysql://localhost:3306/mytest_db?characterEncoding=utf8")
   props.put("username", "root")
   props.put("password", "123456")
    val dataSource: DataSource = DruidDataSourceFactory.createDataSource(props)
   dataSource
 }


  def getConn():DruidPooledConnection={
    source.getConnection
  }

  def releaseConn(conn:Connection)={
    if(conn!=null){
      conn.close()
    }
  }
  def close()={
    source.close()
  }
}
