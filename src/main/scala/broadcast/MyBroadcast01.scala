package broadcast

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
/**
 * @author jiangfan
 * @date 2022/9/7 19:12
 */
//class MyBroadcast {
//
//}

class MyBroadcast01(dimState:MapStateDescriptor[String, String] ) extends BroadcastProcessFunction[String, String, String]() {
//  val dimState = new MapStateDescriptor[String, String]("dimState", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
  /*
                     处理数据流数据
                      */
  def processElement(line: String, readOnlyContext: BroadcastProcessFunction[String, String, String]#ReadOnlyContext, collector: Collector[String]): Unit = {
          println("first======"+line)
          var state = readOnlyContext.getBroadcastState(dimState)
      //        val jn = JSON.parseObject(line)
      //        val vodid = jn.get("vodid").toString
      //        val userid = jn.get("userid").toString
      //        val time = jn.get("time").toString
    if(state==null){
      println("null begin")
    }else{
      println("not null")
    }

          val iterable = state.immutableEntries().iterator()
          while(iterable.hasNext){
            println("element==>"+iterable.next())
          }
          if (state.contains("uuend")) {
          println("包含元素===="+line)
          val vodInfo = state.get("uuend")
      //          val infos = vodInfo.split(",")
      //          val vodName = infos(1)
      //          val vodTag = infos(2)
      //          val vodActor = infos(3)
      //          val joiner = new StringJoiner(",")
      //          joiner.add(vodInfo)
      //          joiner.add(time).add(userid).add(vodid).add(vodName).add(vodTag).add(vodActor)
      //          collector.collect(joiner.toString)
          collector.collect(vodInfo+"--->"+line)
      }
    }

  //                         处理广播流数据
   def processBroadcastElement(s: String, context: BroadcastProcessFunction[String, String, String]#Context, collector:  Collector[String]): Unit = {
       println("second===="+s)
       val state = context.getBroadcastState(dimState)
       val key = s.split(",")(0)
       //        if (!state.contains(key)) {
       println("新的--加入" + key)
       state.put(key, s)
       state.put(key+"end", s+"buff")
       //        }
    }
}