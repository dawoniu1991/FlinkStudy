package wc

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, GroupedDataSet}

object WordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "src\\main\\resources\\hello.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

//    val value01: DataSet[String] = inputDataSet.flatMap(_.split(" "))
//    val value02: DataSet[(String, Int)] = value01.map((_, 1))
//    val value03: GroupedDataSet[(String, Int)] = value02.groupBy(0)
//    val value04: AggregateDataSet[(String, Int)] = value03.sum(1)

    val value04: AggregateDataSet[(String, Int)] = inputDataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    value04.print()
  }
}
