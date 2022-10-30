package TwoPhaseCommitTest

/**
 * @author jiangfan
 * @date 2022/9/27 18:16
 */

import org.apache.flink.api.common.functions.MapFunction

class S2PMapFunction() extends MapFunction[String,(String,String,String)] {

  override def map(value: String): (String, String, String) = {


    var tuple = value.replaceAllLiterally("(","").replaceAllLiterally(")","").split(',')

    (tuple(0),tuple(1),tuple(2))

  }
}
