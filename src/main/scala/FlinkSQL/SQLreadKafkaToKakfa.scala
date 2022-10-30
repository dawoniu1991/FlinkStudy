//package FlinkSQL
//
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.table.api.DataTypes
////import org.apache.flink.table.api.scala._
//import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
////运行作业  ./bin/flink run  -c FlinkSQL.SQLreadKafkaToKakfa   /mnt/f/myflinkstudy/FlinkStudy/target/FlinkStudy-1.0-SNAPSHOT-jar-with-dependencies.jar
//
//object SQLreadKafkaToKakfa {
//  def main(args: Array[String]): Unit = {
//    // 1. 创建环境
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//
//    val tableEnv = StreamTableEnvironment.create(env)
//
//    // 2. 从kafka读取数据
//    tableEnv.connect(new Kafka()
//      .version("universal")
//      .topic("first001")
//      .property("zookeeper.connect", "localhost:2181")
//      .property("bootstrap.servers", "localhost:9092")
//    )
//      .withFormat(new Csv())
//      .withSchema(new Schema()
//        .field("dataid", DataTypes.STRING())
//      )
//      .createTemporaryTable("kafkaInputTable")
//
//    val resultTable = tableEnv.from("kafkaInputTable")
//
//    // 4. 输出到kafka
//    tableEnv.connect(new Kafka()
//      .version("universal")
//      .topic("output01")
//      .property("zookeeper.connect", "localhost:2181")
//      .property("bootstrap.servers", "localhost:9092")
//    )
//      .withFormat(new Csv())
//      .withSchema(new Schema()
//        .field("username", DataTypes.STRING())
//      )
//      .createTemporaryTable("kafkaOutputTable")
//
//    resultTable.insertInto("kafkaOutputTable")
//
//    env.execute("SQLreadKafkaToKakfa test")
//  }
//}
