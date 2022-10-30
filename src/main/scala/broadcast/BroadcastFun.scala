package broadcast

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

import java.{lang, util}
import java.util.StringJoiner
import scala.collection.JavaConversions._
/**
 * @author jiangfan
 * @date 2022/10/10 18:14
 */
class BroadcastFun(mapstate: MapStateDescriptor[String, util.Map[String, String]] ) extends KeyedBroadcastProcessFunction[String,(String, JSONObject),java.util.Map[String, String],JSONObject]{
  lazy val accumulateListState: ListState[(String, JSONObject)] = getRuntimeContext.getListState(new ListStateDescriptor[(String, JSONObject)]("accumulate-list", classOf[(String, JSONObject)]))
  lazy val timeserviceState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time-trigger", classOf[Long]))


  override def processElement(value: (String, JSONObject), ctx: KeyedBroadcastProcessFunction[String, (String, JSONObject), util.Map[String, String], JSONObject]#ReadOnlyContext, out: Collector[JSONObject]): Unit = {
    val redisData: util.Map[String, String] = ctx.getBroadcastState(mapstate).get("redisData")
    val waitingTime=1000*60L
    if(redisData==null){
       accumulateListState.add(value)
      val triggerTime=System.currentTimeMillis() + waitingTime
      if(timeserviceState.value()==0){
        ctx.timerService().registerProcessingTimeTimer(triggerTime)
        timeserviceState.update(triggerTime)
      }else if(timeserviceState.value()<triggerTime){
        ctx.timerService().deleteProcessingTimeTimer(timeserviceState.value())
        ctx.timerService().registerProcessingTimeTimer(triggerTime)
        timeserviceState.update(triggerTime)
      }
    }else{
      val rule=getExistRule(value._1,redisData)
      if(rule.nonEmpty){
        value._2.put("serviceType",rule)
        out.collect(value._2)
      }
    }
  }

  override def processBroadcastElement(value: util.Map[String, String], ctx: KeyedBroadcastProcessFunction[String, (String, JSONObject), util.Map[String, String], JSONObject]#Context, out: Collector[JSONObject]): Unit = {
    val state = ctx.getBroadcastState(mapstate)
    state.clear()
    state.put("redisData",value)
  }
  override def onTimer(timestamp: Long, ctx: KeyedBroadcastProcessFunction[String, (String, JSONObject), util.Map[String, String], JSONObject]#OnTimerContext, out: Collector[JSONObject]): Unit = {
    val redisData: util.Map[String, String] = ctx.getBroadcastState(mapstate).get("redisData")
    if(redisData==null){

    }else{
      val accumulateList: lang.Iterable[(String, JSONObject)] = accumulateListState.get()
      for(item<-accumulateList){
        val rule=getExistRule(item._1,redisData)
        if(rule.nonEmpty){
          out.collect(item._2)
        }
      }
      accumulateListState.clear()
      timeserviceState.clear()
    }
  }

  def getExistRule(value:String,redisData: util.Map[String, String] ): String ={
    try{
       for(item<-redisData){
         val strings = item._1.split("\\|")
         val valueStrings = value.split("\\|")
         if(value.contains(strings(3)) && value.contains(item._1)){
           return item._2
         }
       }
       ""
    }catch{
      case e:Exception=> println(e.getMessage,e)
        ""
    }
  }
}
