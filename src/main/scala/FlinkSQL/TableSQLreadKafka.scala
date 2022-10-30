//package FlinkSQL
//
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.table.api.DataTypes
//import org.apache.flink.table.api.bridge.scala.tableConversions
////import org.apache.flink.table.api.scala._
//import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
///**
// * @author jiangfan
// * @date 2022/10/17 20:05
// */
//object TableSQLreadKafka {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//    val tableEnv = StreamTableEnvironment.create(env)
//
//    // 连接到Kafka
//    tableEnv.connect( new Kafka()
//      .version("0.11")
//      .topic("sensor")
//      .property("zookeeper.connect", "localhost:2181")
//      .property("bootstrap.servers", "localhost:9092")
//    )
//      .withFormat( new Csv() )
//      .withSchema( new Schema()
//        .field("id", DataTypes.STRING())
//        .field("temperature", DataTypes.STRING()) )
//      .createTemporaryTable("kafkaInputTable")
//
//
//    val resultSqlTable = tableEnv.sqlQuery(
//      """
//        |select id, temperature
//        |from kafkaInputTable
//      """.stripMargin)
//
//    resultSqlTable.toAppendStream[(String, Double)].print("res")
//    env.execute()
//  }
//}
