//package FlinkSQL
//
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.table.api.Table
//import org.apache.flink.table.api.scala.{StreamTableEnvironment, tableConversions}
//import org.apache.flink.types.Row
////windows topic生产数据  .\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic test001
////  windows查看 消费进度  .\bin\windows\kafka-consumer-groups.bat  --bootstrap-server localhost:9092 --group  jf-test1016-02 --describe
//object SQLreadKafka1_10_1 {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//    val tableEnv = StreamTableEnvironment.create(env)
////    30,1547719410,sensor_294
//    val createTableSql =
//      """CREATE TABLE mykafka (
//        |    myid string,
//        |    myts string,
//        |    sensor string
//        |) WITH (
//        |    'connector.type' = 'kafka',
//        |    'connector.version' = 'universal',
//        |	   'connector.topic' = 'test001',
//        |	   'connector.properties.bootstrap.servers' = 'localhost:9092',
//        |    'connector.properties.group.id' = 'jf-test1016-02',
//        |	   'format.type' = 'csv'
//        |   )
//        """.stripMargin
////	   'connector.startup-mode' = 'latest-offset',
//    tableEnv.sqlUpdate(createTableSql)
//    val query: String =
//      """SELECT
//        |  myid,myts,sensor
//        |FROM
//        |  mykafka""".stripMargin
//    val resultSqlTable: Table = tableEnv.sqlQuery(query)
//    val result = resultSqlTable.toAppendStream[Row]
//    result.print()
//    env.execute("Flink 1101SQL")
//  }
//}
