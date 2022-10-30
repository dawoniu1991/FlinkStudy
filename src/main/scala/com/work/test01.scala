package com.work

/**
 * @author jiangfan
 * @date 2022/6/22 13:33
 */



  import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
  import org.apache.flink.streaming.api.TimeCharacteristic
  import org.apache.flink.streaming.api.functions.KeyedProcessFunction
  import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
  import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
  import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
  import org.apache.flink.streaming.api.windowing.time.Time
  import org.apache.flink.streaming.api.windowing.windows.TimeWindow
  import org.apache.flink.util.Collector

  import java.lang
  import java.text.SimpleDateFormat
  import java.util.Date

  /**
   * @author jiangfan
   * @date 2022/6/19 15:51
   */
  object test01 {
    def main(args: Array[String]): Unit = {



      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

      //     读取数据并转换成样例类类型，并且提取时间戳设置watermark
      val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)
      //        val inputStream: DataStream[String] = env.readTextFile("src\\main\\resources\\job01.csv")

      val dataStream: DataStream[(String, String, Int)] = inputStream
        .map( line => {
          val arr = line.split(",")
          Tuple3( arr(0), arr(1), arr(2).toInt )
        } ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, String, Int)](Time.seconds(0)) {
        override def extractTimestamp(element: (String, String, Int)): Long = funStringToTimeStamp(element._2)
      }
      )
      //      .assignAscendingTimestamps( _.1 * 1000L )
      //    dataStream.keyBy(_._1).process(new averagrTempProcessFun1()).print("res=")
      dataStream.keyBy(_._1).timeWindow(Time.days(1)).allowedLateness(Time.hours(1)).process(new averagrTempWindowProcessFun()).print("res=")
      //    dataStream.keyBy(_._1).timeWindow(Time.days(1)).
      env.execute("job01=====")

      //    println(funStringToTimeStamp("2020-01-30 19:00:01"))


    }

    def funStringToTimeStamp(time: String): Long = {
      val fm = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val dt = fm.parse(time)
      dt.getTime
    }

  }

  class averagrTempProcessFun1()  extends KeyedProcessFunction[String,(String, String, Int),(String, String, Double)]{

    lazy val counter: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("counter",classOf[Long]))
    lazy val sumTemp: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("sum-Temp",classOf[Long]))

    override def processElement(value: (String, String, Int), ctx: KeyedProcessFunction[String, (String, String, Int), (String, String, Double)]#Context, out: Collector[(String, String, Double)]): Unit = {

      if(counter.value()==null){
        counter.update(1)
      }else{
        counter.update(counter.value()+1)
      }

      if(sumTemp.value()==null){
        sumTemp.update(value._3)
      }else{
        sumTemp.update(sumTemp.value()+value._3)
      }
      val nowtime: lang.Long = ctx.timestamp()
      println("time===="+nowtime)
      val trigger=getEndDateTimestamp(nowtime)
      println("trigger================"+trigger)
      ctx.timerService().registerEventTimeTimer(trigger)
    }


    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, String, Int), (String, String, Double)]#OnTimerContext, out: Collector[(String, String, Double)]): Unit = {
      var averageTemp=0d
      println("sum=="+sumTemp.value())
      println("count=="+counter.value())
      if( sumTemp.value()!=null && counter.value()!=null ){
        averageTemp=sumTemp.value().toDouble/counter.value()
      }
      out.collect((ctx.getCurrentKey,ctx.timestamp().toString,averageTemp))
      sumTemp.clear()
      counter.clear()
    }

    private def getEndDateTimestamp(curTime:Long) = {
      val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      //    val curTime = System.currentTimeMillis()
      val hourTime = df.format(curTime)
      val hourTimestamp = df.parse(hourTime).getTime
      hourTimestamp + 86400000
    }
  }

  class averagrTempWindowProcessFun  extends ProcessWindowFunction[(String, String, Int),(String, String, Double),String,TimeWindow]{
    lazy val counter: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("counter",classOf[Long]))
    lazy val sumTemp: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("sum-Temp",classOf[Long]))
    //  override def process(key: (String, String, Double), context: Context, elements: Iterable[String], out: Collector[(String, String, Int)]): Unit = {
    //    println("jieshu====="+context.window.getEnd)
    //    while(elements)
    //
    //  }
    override def process(key: String, context: Context, elements: Iterable[(String, String, Int)], out: Collector[(String, String, Double)]): Unit = {
      println("jieshu====="+context.window.getEnd)
      println("size========"+elements.size)
      //    elements.foreach(println(_))
      val (cnt, sum): (Int, Double) = elements.foldLeft((10, 200.0))((c, r) => (c._1 + 1, c._2 + r._3))
      out.collect(("aa",cnt.toString,sum.toDouble))
      val iterator: Iterator[(String, String, Int)] = elements.iterator
      while(iterator.hasNext){
        iterator.next()
      }
    }
  }


