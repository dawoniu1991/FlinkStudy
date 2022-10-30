//package FlinkSQL
//
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.table.api.bridge.scala.tableConversions
////import org.apache.flink.table.api.scala._
//import org.apache.flink.types.Row
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
///**
// * @author jiangfan
// * @date 2022/10/16 15:19
// */
//object test003 {
//  case class Event(username: String, url: String, myts: Long)
//  case class MyItem(uid: String, itemUrl: String, itemTs: Long)
//  def main(args: Array[String]): Unit = {
//    // 1. 创建环境
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    //    env.setParallelism(1)
//
//    val tableEnv = StreamTableEnvironment.create(env)
//
//    // 2. 在将流转换成表的时候指定时间属性字段
////          Alice, ./home, 1000
////          Alice, ./prod?id=1, 2000
////          Alice, ./prod?id=4, 3000
////          Cary, ./home, 6000
////          Cary, ./prod?id=7, 36000
//
//    val eventStream = env.socketTextStream("localhost", 7777).map({ line =>
//      val strings = line.split(",")
//      Event(strings(0).trim, strings(1).trim, strings(2).trim.toLong)
//    })
//
//    val itemStream = env.socketTextStream("localhost", 8888).map({ line =>
//      val strings = line.split(",")
//      MyItem(strings(0).trim, strings(1).trim, strings(2).trim.toLong)
//    })
//    val eventTable = tableEnv.fromDataStream(eventStream)
//    eventTable.printSchema()
//
////         Cary, aaaaa, 60
////         Alice, bbbbb, 10
////         Cary, ccccc, 360
////         Alice, ddddd, 20
////         Alice, eeeee, 3
//
//    val itemTable = tableEnv.fromDataStream(itemStream)
//    itemTable.printSchema()
//
//    tableEnv.createTemporaryView("inputTable", eventTable)
//    tableEnv.createTemporaryView("inputTable02", itemTable)
//
//    val resultSqlTable = tableEnv.sqlQuery(
//      """
//        |select
//        |  *
//        |from
//        |  inputTable  join inputTable02
//        |  on inputTable.username=inputTable02.uid
//          """.stripMargin)
//    //17600406785
//    //        resultSqlTable.toRetractStream[Row].print("sql")
//    resultSqlTable.toAppendStream[Row].print("sql")
//
//    env.execute("table function test")
//  }
//}
