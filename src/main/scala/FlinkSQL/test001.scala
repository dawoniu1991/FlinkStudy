package FlinkSQL

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.tableConversions
//import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
/**
 * @author jiangfan
 * @date 2022/10/16 13:19
 */
//bin/flink run  -c FlinkSQL.test001   /mnt/f/myflinkstudy/FlinkStudy/target/FlinkStudy-1.0-SNAPSHOT-jar-with-dependencies.jar

object test001 {
  case class Event(user: String, url: String, myts: Long)
  def main(args: Array[String]): Unit = {
    // 1. 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    // 2. 在将流转换成表的时候指定时间属性字段
    val eventStream = env.fromElements(
      Event("Alice", "./home", 1000L),
      Event("Bob", "./cart", 1000L),
      Event("Alice", "./prod?id=1", 2000L),
      Event("Alice", "./prod?id=4", 3000L),
      Event("Bob", "./prod?id=5", 5000L),
      Event("Cary", "./home", 6000L),
      Event("Cary", "./prod?id=7", 36000L)
    )

    val eventTable = tableEnv.fromDataStream(eventStream)
    eventTable.printSchema()

    tableEnv.createTemporaryView("inputTable", eventTable)
//    val resultSqlTable = tableEnv.sqlQuery(
//      """
//        |select
//        |  user, url, myts
//        |from
//        |  inputTable
//      """.stripMargin)

//    resultSqlTable.toAppendStream[Row].print("sql")

    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select
        |  user,  sum(myts)
        |from
        |  inputTable group by user
      """.stripMargin)

    resultSqlTable.toRetractStream[Row].print("sql")


    env.execute("table function test")

  }
}
