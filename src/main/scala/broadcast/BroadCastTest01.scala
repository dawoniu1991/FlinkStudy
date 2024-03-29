package broadcast

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.scala._

/**
 * @author jiangfan
 * @date 2022/9/7 19:11
 */
object BroadCastTest01 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val value01 = env.socketTextStream("localhost", 7777)
    val value02 = env.socketTextStream("localhost", 8888)
    // 声明一个MapStateDescriptor，维度表作为广播state
    val dimState = new MapStateDescriptor[String, String]("dimState", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
    val broadcastStream = value02.broadcast(dimState)
    val output = value01.connect(broadcastStream).process(new MyBroadcast01(dimState))
    output.print("res=====")
    env.execute("my")
  }
}
