package scalaTest01

import com.alibaba.druid.pool.DruidPooledConnection
import utils.DruidDSUtil01

import java.sql.Connection

/**
 * @author jiangfan
 * @date 2022/10/11 11:10
 */
object ScalaTest006 {
  class Person

  class Student extends Person {

    def sayHello(): Unit = println("Hello, yeah")

  }

  def main(args: Array[String]): Unit = {
    val p: Person = new Student
    p match {
      case s: Student =>
        println("aaaaa")
        s.sayHello()
      case _ =>
    }

    val druidDataSource = DruidDSUtil01.getDataSource()
    druidDataSource.setDefaultAutoCommit(false)
    val connection: DruidPooledConnection =druidDataSource.getConnection()
    val state = new ConnectionState(connection)
    println(state.connection)

    /**
     * 新写法
     */
    /*p match {
      case s: Student =>
        s.sayHello()
    }*/
  }

  class ConnectionState( @transient val  connection: Connection ) {

  }

}
