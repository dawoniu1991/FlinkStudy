//package FlinkSQL
//
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.table.api.bridge.scala.tableConversions
////import org.apache.flink.table.api.scala._
//import org.apache.flink.types.Row
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
///**
// * @author jiangfan
// * @date 2022/10/16 13:53
// */
//object test002 {
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
//    val eventStream = env.fromElements(
//      Event("Alice", "./home", 1000L),
//      Event("Alice", "./prod?id=1", 2000L),
//      Event("Alice", "./prod?id=4", 3000L),
//      Event("Cary", "./home", 6000L),
//      Event("Cary", "./prod?id=7", 36000L)
//    )
//
//    val eventTable = tableEnv.fromDataStream(eventStream)
//    eventTable.printSchema()
//
//    val itemStream = env.fromElements(
//      MyItem("Cary", "aaaaa", 60L),
//      MyItem("Alice", "bbbbb", 10L),
//      MyItem("Cary", "ccccc", 360L),
//      MyItem("Alice", "ddddd", 20L),
//      MyItem("Alice", "eeeee", 30L)
//    )
//
//    val itemTable = tableEnv.fromDataStream(itemStream)
//    itemTable.printSchema()
//
//    tableEnv.createTemporaryView("inputTable", eventTable)
//    tableEnv.createTemporaryView("inputTable02", itemTable)
//
////        val resultSqlTable = tableEnv.sqlQuery(
////          """
////            |select
////            |  *
////            |from
////            |  inputTable  join inputTable02
////            |  on inputTable.username=inputTable02.uid  where inputTable.username='Alice' and inputTable.url='./home'
////          """.stripMargin)
//val resultSqlTable = tableEnv.sqlQuery(
//  """
//    |select
//    |  *
//    |from
//    |  inputTable  , inputTable02
//          """.stripMargin)
////17600406785
////        resultSqlTable.toRetractStream[Row].print("sql")
//    resultSqlTable.toAppendStream[Row].print("sql")
//
//
//    env.execute("table function test")
//
//  }
//}
