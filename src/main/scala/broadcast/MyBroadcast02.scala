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

class MyBroadcast02(dimState:MapStateDescriptor[String, String] ) extends BroadcastProcessFunction[String, String, String]() {
  //  val dimState = new MapStateDescriptor[String, String]("dimState", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
  /*
                     处理数据流数据
                      */

  def processElement(line: String, readOnlyContext: BroadcastProcessFunction[String, String, String]#ReadOnlyContext, collector: Collector[String]): Unit = {
    println("first======"+line)
    var state = readOnlyContext.getBroadcastState(dimState).get("mydata")

    //        val jn = JSON.parseObject(line)
    //        val vodid = jn.get("vodid").toString
    //        val userid = jn.get("userid").toString
    //        val time = jn.get("time").toString
    if(state==null){
      println("null begin")
    }else{
      println("not null")
    }
    while(state==null){
      Thread.sleep(10000)
      println("sleep=="+line)
      state=readOnlyContext.getBroadcastState(dimState).get("mydata")
    }

    if (state.contains("yyu")) {
      println("包含元素===="+line)

      collector.collect(state+"--->"+line)
    }
  }

  //                         处理广播流数据
  def processBroadcastElement(s: String, context: BroadcastProcessFunction[String, String, String]#Context, collector:  Collector[String]): Unit = {
    println("second===="+s)
    val state = context.getBroadcastState(dimState)
//    val key = s.split(",")(0)
    val key ="mydata"
    //        if (!state.contains(key)) {
    println("新的--加入" + key)
    state.put(key, s)
    state.put(key+"end", s+"suffix")
    //        }
  }
}