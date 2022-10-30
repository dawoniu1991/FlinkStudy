//package FlinkSQL
//
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.table.api.Table
//import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
//import org.apache.flink.types.Row
//// windows 查看kafka消费进度
//// .\bin\windows\kafka-consumer-groups.bat  --bootstrap-server localhost:9092 --group  jf-test1016 --describe
///**
// * @author jiangfan
// * @date 2022/10/17 16:56
// */
// //运行作业  ./bin/flink run  -c FlinkSQL.SQLreadKafka   /mnt/f/myflinkstudy/FlinkStudy/target/FlinkStudy-1.0-SNAPSHOT-jar-with-dependencies.jar
//// 下面是flink 1.11.2版本的写法
//object SQLreadKafka1_11_2 {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//        env.setParallelism(1)
//    val tableEnv = StreamTableEnvironment.create(env)
//30,1547719410,sensor_294
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
//        |    'connector.properties.group.id' = 'jf-test1016',
//        |    'connector.properties.auto.offset.reset' = 'latest',
//        |	   'format.type' = 'csv'
//        |   )
//        """.stripMargin

//        |	   'connector.startup-mode' = 'latest-offset',

//    tableEnv.executeSql(createTableSql)
//    val query: String =
//      """SELECT
//        |  myid,myts,sensor
//        |FROM
//        |  mykafka""".stripMargin
//    val resultSqlTable: Table = tableEnv.sqlQuery(query)
//    val result = resultSqlTable.toAppendStream[Row]
//    result.print()
//    env.execute("Flink SQL DDL")
//
////    flink 1.13写法
////    val createTable =
////      """
////              CREATE TABLE PERSON (
////        |    name VARCHAR COMMENT '姓名',
////        |    age VARCHAR COMMENT '年龄',
////        |    city VARCHAR COMMENT '所在城市',
////        |    address VARCHAR COMMENT '家庭住址',
////        |    ts BIGINT COMMENT '时间戳',
////        |    pay_time AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')), -- 定义事件时间
////        |    WATERMARK FOR pay_time AS pay_time - INTERVAL '0' SECOND
////              )
////        |WITH (
////        |    'connector.type' = 'kafka', -- 使用 kafka connector
////        |    'connector.version' = 'universal',  -- kafka 版本
////        |    'connector.topic' = 'kafka_ddl',  -- kafka topic
////        |    'connector.startup-mode' = 'earliest-offset', -- 从最早的 offset 开始读取
////        |    'connector.properties.0.key' = 'zookeeper.connect',  -- 连接信息
////        |    'connector.properties.0.value' = 'Desktop:2181',
////        |    'connector.properties.1.key' = 'bootstrap.servers',
////        |    'connector.properties.1.value' = 'Desktop:9091',
////        |    'update-mode' = 'append',
////        |    'format.type' = 'json',  -- 数据源格式为 json
////        |    'format.derive-schema' = 'true' -- 从 DDL schema 确定 json 解析规则
////        |)
////            """.stripMargin
//
//  }
//}
