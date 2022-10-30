package scalaTest01

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
 * @author jiangfan
 * @date 2022/9/29 19:38
 */
object ScalaTest003 {
  def main(args: Array[String]): Unit = {
    val url="jdbc:mysql://localhost:3306/mytest_db"
    val user="root"
    val password="123456"
    Class.forName("com.mysql.jdbc.Driver")
    val connection: Connection = DriverManager.getConnection(url, user, password)
    connection.setAutoCommit(false)

    val sql = "INSERT INTO t_book ( user_id ,username,ustatus) values (?,?,?)"
    val statement = connection.prepareStatement(sql)
    statement.setString(1,"aa")
    statement.setString(2,"test")
    statement.setInt(3,99)
    statement.execute()
    statement.close()
    connection.commit()
    connection.close()

//    val rs: ResultSet = prepareStatement.executeQuery("select * from blacklist")
//
//    try {
//      while (rs.next()) {
//        println(rs.getInt("id") + ":" + rs.getNString("name"))
//      }
//    } catch {
//      case e => e.printStackTrace()
//    }finally {
//      rs.close()
//      prepareStatement.close()
//      connection.close()
//    }

  }
}
