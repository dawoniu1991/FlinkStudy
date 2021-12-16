package mytest

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala.createTypeInformation

/**
 * @author jiangfan
 * @date 2021/5/18 17:43
 */
//object test07 {
//
//}


class CountWindowAverage extends RichFlatMapFunction[(Long, Long), (Long, Long)] {

  private var sum: ValueState[(Long, Long)] = _

//  override def flatMap(input: (Long, Long), out: Collector[(Long, Long)]): Unit = {
//    val tmpCurrentSum = sum.value
//    println("tmpCurrentSum==="+tmpCurrentSum)
//    println("input======"+input)
//
//    val currentSum = if (tmpCurrentSum != null) {
//      println("input0011======"+input)
//      tmpCurrentSum
//    } else {
//      (0L, 0L)
//    }
//
//    val newSum = (currentSum._1 + 1, currentSum._2 + input._2)
//    sum.update(newSum)
//    if (newSum._1 >= 2) {
//      out.collect((input._1, newSum._2 / newSum._1))
//      sum.clear()
//    }
//  }

  override def flatMap(input: (Long, Long), out: Collector[(Long, Long)]): Unit = {
    val tmpCurrentSum = sum.value
    println("tmpCurrentSum==="+tmpCurrentSum)
    println("input======"+input)

    val currentSum = if (tmpCurrentSum != null) {
      println("input0011======"+input)
      tmpCurrentSum
    } else {
      (0L, 0L)
    }

    val newSum = (currentSum._1 + input._1, currentSum._2 + input._2)
    sum.update(newSum)
    if (newSum._1 >= 2) {
      out.collect((newSum._1, newSum._2 ))
    }
  }


  override def open(parameters: Configuration): Unit = {
    println("this is open~~~~~~~~~~~~~~~~~~~~~~")
    sum = getRuntimeContext.getState(
      new ValueStateDescriptor[(Long, Long)]("average", createTypeInformation[(Long, Long)])
    )
  }
}


object ExampleCountWindowAverage extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(3)

  env.fromCollection(List(
    (1L, 3L),
    (1L, 5L),
    (1L, 7L),
    (1L, 4L),
    (1L, 2L)
  )).keyBy(_._1)
    .flatMap(new CountWindowAverage())
    .print()
  // the printed output will be (1,4) and (1,5)
//  env.fromCollection(List(
//    (1L, 3L),
//    (1L, 5L),
//    (1L, 7L),
//    (1L, 4L),
//    (1L, 2L)
//  )).keyBy(_._1).print()

  env.execute("ExampleKeyedState")
}



// 1个并行度的输出
//this is open~~~~~~~~~~~~~~~~~~~~~~
//tmpCurrentSum===null
//input======(1,3)
//tmpCurrentSum===(1,3)
//input======(1,5)
//input0011======(1,5)
//(1,4)
//tmpCurrentSum===null
//input======(1,7)
//tmpCurrentSum===(1,7)
//input======(1,4)
//input0011======(1,4)
//(1,5)
//tmpCurrentSum===null
//input======(1,2)


//8个并行度的输出
//this is open~~~~~~~~~~~~~~~~~~~~~~
//this is open~~~~~~~~~~~~~~~~~~~~~~
//this is open~~~~~~~~~~~~~~~~~~~~~~
//this is open~~~~~~~~~~~~~~~~~~~~~~
//this is open~~~~~~~~~~~~~~~~~~~~~~
//this is open~~~~~~~~~~~~~~~~~~~~~~
//this is open~~~~~~~~~~~~~~~~~~~~~~
//this is open~~~~~~~~~~~~~~~~~~~~~~
//tmpCurrentSum===null
//input======(1,3)
//tmpCurrentSum===(1,3)
//input======(1,5)
//input0011======(1,5)
//6> (1,4)
//tmpCurrentSum===null
//input======(1,7)
//tmpCurrentSum===(1,7)
//input======(1,4)
//input0011======(1,4)
//6> (1,5)
//tmpCurrentSum===null
//input======(1,2)