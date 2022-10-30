//package FlinkSQL
//
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.table.api.bridge.scala.tableConversions
////import org.apache.flink.table.api.scala._
//import org.apache.flink.types.Row
//import FlinkSQL.test003.Event
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
///**
// * @author jiangfan
// * @date 2022/10/16 15:37
// */
//object test004 {
//  case class Event(user: String, url: String, myts: Long)
//  def main(args: Array[String]): Unit = {
//    // 1. 创建环境
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    //    env.setParallelism(1)
//
//    val tableEnv = StreamTableEnvironment.create(env)
//
//    val eventStream = env.socketTextStream("localhost", 7777).map({ line =>
//      val strings = line.split(",")
//      Event(strings(0).trim, strings(1).trim, strings(2).trim.toLong)
//    })
//    val eventTable = tableEnv.fromDataStream(eventStream)
//    eventTable.printSchema()
//
//    tableEnv.createTemporaryView("inputTable", eventTable)
//    //    val resultSqlTable = tableEnv.sqlQuery(
//    //      """
//    //        |select
//    //        |  user, url, myts
//    //        |from
//    //        |  inputTable
//    //      """.stripMargin)
//
//    //    resultSqlTable.toAppendStream[Row].print("sql")
//
//    //          Alice, ./home, 1000
//    //          Alice, ./prod?id=1, 2000
//    //          Alice, ./prod?id=4, 3000
//    //          Cary, ./home, 6000
//    //          Cary, ./prod?id=7, 36000
//
//    val resultSqlTable = tableEnv.sqlQuery(
//      """
//        |select
//        |  user,  sum(myts)
//        |from
//        |  inputTable group by user
//      """.stripMargin)
//
////    resultSqlTable.toRetractStream[Row].print("sql")
//    val result: DataStream[(Boolean, Row)] = resultSqlTable.toRetractStream[Row]
//    result.print()
//
//    env.execute("table function test")
//
//  }
//}
