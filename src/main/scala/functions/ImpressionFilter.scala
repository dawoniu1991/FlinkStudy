package functions

import org.apache.flink.api.common.functions.RichFilterFunction

class ImpressionFilter extends RichFilterFunction[(String, String)] {

  /**
   * 实现具体的过滤规则
   *
   * @param value 展现日志
   * @return 数据是否合法
   */
  override def filter(value: (String, String)): Boolean = {
    val imp = value._2
    imp.contains("aaa")
  }

}