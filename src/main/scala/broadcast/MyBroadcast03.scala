package broadcast

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

import java.lang

/**
 * @author jiangfan
 * @date 2022/9/8 11:12
 */
class MyBroadcast03(dimState:MapStateDescriptor[String, String] ) extends KeyedBroadcastProcessFunction[String,String, String, String]{

  lazy val accumulateListState: ListState[String] = getRuntimeContext.getListState(new ListStateDescriptor[String]("accumulate-list", classOf[String]))
  lazy val timeserviceState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time-trigger", classOf[Long]))

  override def processElement(value: String, ctx: KeyedBroadcastProcessFunction[String, String, String, String]#ReadOnlyContext, out: Collector[String]): Unit = {
    println("first======"+value)
    val state = ctx.getBroadcastState(dimState).get("mydata")
    val expireTime=1000*30L
    println("timeserviceState.value()==")
    println(timeserviceState.value() == 0)
    println("timeserviceState.value()")
    if(state==null){
      println("null begin")
      accumulateListState.add(value)
      val triggerTime=System.currentTimeMillis() + expireTime
      if(timeserviceState.value()==0){
        ctx.timerService().registerProcessingTimeTimer(triggerTime)
        timeserviceState.update(triggerTime)
        println("111111111111")
      }else if(timeserviceState.value()<triggerTime){
        ctx.timerService().deleteProcessingTimeTimer(timeserviceState.value())
        ctx.timerService().registerProcessingTimeTimer(triggerTime)
        timeserviceState.update(triggerTime)
        println("2222222222222")
      }
    }else{
      println("not null")
      if (state.contains("yyu")) {
        println("包含元素===="+value)
        out.collect(state+"--->"+value)
      }
    }


  }

  override def processBroadcastElement(value: String, ctx: KeyedBroadcastProcessFunction[String, String, String, String]#Context, out: Collector[String]): Unit = {
    println("second===="+value)
    val state = ctx.getBroadcastState(dimState)
    val key ="mydata"
    println("新的--加入" + key)
    state.put(key, value)
    state.put(key+"end", value+"suffix")
  }

  override def onTimer(timestamp: Long, ctx: KeyedBroadcastProcessFunction[String, String, String, String]#OnTimerContext, out: Collector[String]): Unit = {
    import scala.collection.JavaConversions._

    val state = ctx.getBroadcastState(dimState).get("mydata")
    if(state==null){
      println("ontime--state is null")
       return
    }else{
       val accumulateList: lang.Iterable[String] = accumulateListState.get()
       println("ontime--state not null"+state)
       for(data<- accumulateList) {
          println("ontime==="+data)
       }
      // 清空状态
      accumulateListState.clear()
      timeserviceState.clear()
    }


  }
}
