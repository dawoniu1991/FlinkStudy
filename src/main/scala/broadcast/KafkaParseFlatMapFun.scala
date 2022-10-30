package broadcast

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

import java.util.{Properties, StringJoiner}

/**
 * @author jiangfan
 * @date 2022/10/10 18:04
 */
class KafkaParseFlatMapFun(config:Properties) extends RichFlatMapFunction[String,(String,JSONObject)] {
  override def flatMap(value: String, out: Collector[(String, JSONObject)]): Unit = {
    try{
      val jsonObject = JSON.parseObject(value)
      val custId = jsonObject.getString("custId")
      val custInfo = jsonObject.getString("custInfo")
      val custPhone = jsonObject.getString("custPhone")
      val joiner = new StringJoiner("|")
      out.collect(joiner.add(custId).add(custInfo).add(custPhone).toString,jsonObject)
    }catch{
      case e:Exception=> println(e.getMessage,e)
    }
  }
}
