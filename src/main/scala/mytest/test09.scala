package mytest

/**
 * @author jiangfan
 * @date 2022/9/28 16:26
 */
object test09 {
  def main(args: Array[String]): Unit = {
//     testFun("abc")

     var transactionMap : Map[String,Array[(String,String,String,String)]] = Map()
    println(transactionMap)
    println("preCommit--"+transactionMap)
    val transaction="mykey"
    val array=Array(("111","222","33","44"))
    transactionMap += (transaction -> array)
    println(transactionMap)
    println(transactionMap.mkString)
    println("preCommit--"+transactionMap("mykey").mkString)
    println("preCommit--"+transactionMap.mkString)
  }

  def testFun(str:String):String={
    testFun(str)+"1"
  }
}
