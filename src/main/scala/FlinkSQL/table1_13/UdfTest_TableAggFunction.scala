//package tabletest.table1_13
//
//import com.atguigu.chapter11.UdfTest_AggregateFunction.WeightedAvg
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.table.api.Expressions.{$, call}
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//import org.apache.flink.table.functions.TableAggregateFunction
//import org.apache.flink.util.Collector
//
//import java.sql.Timestamp
//
///**
// * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
// *
// * Project:  FlinkTutorial
// *
// * Created by  wushengran
// */
//
//object UdfTest_TableAggFunction {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//
//    val tableEnv = StreamTableEnvironment.create(env)
//
//    // 1. 创建表
//    tableEnv.executeSql("CREATE TABLE eventTable (" +
//      " uid STRING," +
//      " url STRING," +
//      " ts BIGINT," +
//      " et AS TO_TIMESTAMP( FROM_UNIXTIME(ts/1000) )," +
//      " WATERMARK FOR et AS et - INTERVAL '2' SECOND" +
//      ") WITH (" +
//      " 'connector' = 'filesystem'," +
//      " 'path' = 'input/clicks.txt'," +
//      " 'format' = 'csv'" +
//      ") ")
//
//    // 2. 注册表聚合函数
//    tableEnv.createTemporarySystemFunction("top2", classOf[Top2])
//
//    // 3. 调用函数进行查询转换
//    // 首先进行窗口聚合得到cnt值
//    val urlCountWindowTable = tableEnv.sqlQuery(
//      """
//        |SELECT uid, COUNT(url) AS cnt, window_start as wstart, window_end as wend
//        |FROM TABLE (
//        |  TUMBLE(TABLE eventTable, DESCRIPTOR(et), INTERVAL '1' HOUR)
//        |)
//        |GROUP BY uid, window_start, window_end
//        |""".stripMargin)
//
//    // 使用Table API调用表聚合函数
//    val resultTable = urlCountWindowTable.groupBy($("wend"))
//      .flatAggregate(call("top2", $("uid"), $("cnt"), $("wstart"), $("wend")))
//      .select($("uid"), $("rank"), $("cnt"), $("wend"))
//
//    // 4. 结果打印输出
//    tableEnv.toChangelogStream(resultTable).print()
//
//    env.execute()
//  }
//
//  // 定义输出结果和中间累加器的样例类
//  case class Top2Result(uid: String, window_start: Timestamp, window_end: Timestamp, cnt: Long, rank: Int)
//
//  case class Top2Accumulator(var maxCount: Long, var secondMaxCount: Long, var uid1: String, var uid2: String, var window_start: Timestamp, var window_end: Timestamp)
//
//  // 实现自定义的表聚合函数
//  class Top2 extends TableAggregateFunction[Top2Result, Top2Accumulator]{
//    override def createAccumulator(): Top2Accumulator = Top2Accumulator(Long.MinValue, Long.MinValue, null, null, null, null)
//
//    // 每来一行数据，需要使用accumulate进行聚合统计
//    def accumulate(acc: Top2Accumulator, uid: String, cnt: Long, window_start: Timestamp, window_end: Timestamp): Unit ={
//      acc.window_start = window_start
//      acc.window_end = window_end
//      // 判断当前count值是否排名前两位
//      if (cnt > acc.maxCount){
//        // 名次向后顺延
//        acc.secondMaxCount = acc.maxCount
//        acc.uid2 = acc.uid1
//        acc.maxCount = cnt
//        acc.uid1 = uid
//      } else if (cnt > acc.secondMaxCount){
//        acc.secondMaxCount = cnt
//        acc.uid2 = uid
//      }
//    }
//
//    // 输出结果数据
//    def emitValue(acc: Top2Accumulator, out: Collector[Top2Result]): Unit ={
//      // 判断cnt值是否为初始值，如果没有更新过直接跳过不输出
//      if (acc.maxCount != Long.MinValue){
//        out.collect(Top2Result(acc.uid1, acc.window_start, acc.window_end, acc.maxCount, 1))
//      }
//      if (acc.secondMaxCount != Long.MinValue){
//        out.collect(Top2Result(acc.uid2, acc.window_start, acc.window_end, acc.secondMaxCount, 2))
//      }
//    }
//  }
//}
