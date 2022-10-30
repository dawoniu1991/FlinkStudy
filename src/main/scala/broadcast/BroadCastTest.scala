package broadcast

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author jiangfan
 * @date 2022/10/10 16:29
 */
object BroadCastTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val value01 = env.socketTextStream("localhost", 7777)
    val value02 = env.socketTextStream("localhost", 8888)
    // 声明一个MapStateDescriptor，维度表作为广播state
    val dimState = new MapStateDescriptor[String, String]("dimState", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
    val broadcastStream = value02.broadcast(dimState)
    val output = value01.connect(broadcastStream).process(new BroadcastProcessFunction[String, String, String]() {
      /*
                         处理数据流数据
                          */
      def processElement(line: String, readOnlyContext: BroadcastProcessFunction[String, String, String]#ReadOnlyContext, collector: Collector[String]): Unit = {
        println("第一个===" + line)
        val state = readOnlyContext.getBroadcastState(dimState)
        //        val jn = JSON.parseObject(line)
        //        val vodid = jn.get("vodid").toString
        //        val userid = jn.get("userid").toString
        //        val time = jn.get("time").toString
        val iterable = state.immutableEntries().iterator()
        while (iterable.hasNext) {
          println("element==>" + iterable.next())
        }
        if (state.contains("uuend")) {
          println("包含===" + line)
          val vodInfo = state.get("uuend")
          //          val infos = vodInfo.split(",")
          //          val vodName = infos(1)
          //          val vodTag = infos(2)
          //          val vodActor = infos(3)
          //          val joiner = new StringJoiner(",")
          //          joiner.add(vodInfo)
          //          joiner.add(time).add(userid).add(vodid).add(vodName).add(vodTag).add(vodActor)
          //          collector.collect(joiner.toString)
          collector.collect(vodInfo + "--->" + line)
        }
      }

      //                         处理广播流数据
      def processBroadcastElement(s: String, context: BroadcastProcessFunction[String, String, String]#Context, collector: Collector[String]): Unit = {
        println("second====" + s)
        val state = context.getBroadcastState(dimState)
        val key = s.split(",")(0)
        //        if (!state.contains(key)) {
        println("新的vod加入" + key)
        state.put(key, s)
        state.put(key + "end", s + "buff")
        //        }
      }
    })
    output.print("res==")
    env.execute("my")
  }
}
