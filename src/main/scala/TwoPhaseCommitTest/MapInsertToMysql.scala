package TwoPhaseCommitTest

import org.apache.flink.api.common.functions.RichMapFunction
import utils.MysqlConnection

/**
 * @author jiangfan
 * @date 2022/9/29 19:12
 */
class MapInsertToMysql extends RichMapFunction[(String,String,String,String),String]{
  override def map(value: (String, String, String, String)): String = {
    val url="jdbc:mysql://localhost:3306/mytest_db"
    val user="root"
    val password="123456"
    val driver = "com.mysql.jdbc.Driver"
    MysqlConnection.initDruidSource(user,password,driver,url)
    val connection = MysqlConnection.getConn()
    val sql = "INSERT INTO t_book ( user_id ,username,ustatus) values (?,?,?)"
    val statement = connection.prepareStatement(sql)
    statement.setString(1,"aa")
    statement.setString(2,    value._4)
    statement.setInt(3,666)
    statement.execute()
    statement.close()
    println("sleep begin")
    Thread.sleep(20000)
    println("sleep end")
    connection.commit()
    MysqlConnection.releaseConn(connection)

    value._4
  }
}
