package scalaTest01

import utils.MysqlConnection

/**
 * @author jiangfan
 * @date 2022/9/29 15:15
 */
object ScalaTest002 {
  def main(args: Array[String]): Unit = {
    val url="jdbc:mysql://localhost:3306/mytest_db"
    val user="root"
    val password="123456"
    val driver = "com.mysql.jdbc.Driver"
    MysqlConnection.initDruidSource(user,password,driver,url)
    val connection = MysqlConnection.getConn()
    val sql = "INSERT INTO t_book ( user_id ,username,ustatus) values (?,?,?)"
    val statement = connection.prepareStatement(sql)
    statement.setString(1,"aa")
    statement.setString(2,"ceshi")
    statement.setInt(3,666)
    statement.execute()
    statement.close()

    connection.commit()
//    connection.rollback()
    MysqlConnection.releaseConn(connection)
//    connection.
  }
}
