//package mytest.async;
//
///**
// * @author jiangfan
// * @date 2022/6/17 21:35
// */
//
//import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.ReduceFunction;
//import org.apache.flink.api.common.state.ReducingState;
//import org.apache.flink.api.common.state.ReducingStateDescriptor;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.util.Collector;
//
//import java.time.Duration;
//import java.util.Random;
//
//public class ReducingStateTest {
//    public static void main(String[] args) throws Exception {
//        // 需求：取10秒内最高的温度进行输出
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.getConfig().setAutoWatermarkInterval(100l);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        DataStreamSource<Tuple3<String, Integer, Long>> tuple3DataStreamSource = env.addSource(new SourceFunction<Tuple3<String, Integer, Long>>() {
//            boolean flag = true;
//
//            @Override
//            public void run(SourceContext<Tuple3<String, Integer, Long>> ctx) throws Exception {
//                String[] str = {"水阀1", "水阀2", "水阀3"};
//                while (flag) {
//                    int i = new Random().nextInt(3);
//                    // 温度
//                    int temperature = new Random().nextInt(100);
//                    Thread.sleep(1000l);
//                    // 设备号、温度、事件时间
//                    ctx.collect(new Tuple3<String, Integer, Long>(str[i], temperature, System.currentTimeMillis()));
//                }
//            }
//
//            @Override
//            public void cancel() {
//                flag = false;
//            }
//        });
//
//        tuple3DataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
//                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Long>>() {
//                    @Override
//                    public long extractTimestamp(Tuple3<String, Integer, Long> stringIntegerLongTuple3, long l) {
//                        return stringIntegerLongTuple3.f2;
//                    }
//                })).keyBy(new KeySelector<Tuple3<String, Integer, Long>, String>() {
//            @Override
//            public String getKey(Tuple3<String, Integer, Long> stringIntegerLongTuple3) throws Exception {
//                return stringIntegerLongTuple3.f0;
//            }
//        }).process(new KeyedProcessFunction<String, Tuple3<String, Integer, Long>, String>() {
//            Long interval = 10 * 1000l;
//            ReducingState<Integer> reducingState = null;
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//                ReducingStateDescriptor<Integer> reducingStateDescriptor = new ReducingStateDescriptor<Integer>("reducingState",new Max(),Integer.class);
//                reducingState = getRuntimeContext().getReducingState(reducingStateDescriptor);
//            }
//
//            @Override
//            public void processElement(Tuple3<String, Integer, Long> value, Context ctx, Collector<String> out) throws Exception {
//                // 注册定时器10s触发一次,相同定时器重复注册会忽略
//                Long statrTimestamp = ctx.timestamp() - (ctx.timestamp() % interval);
//                Long timerTimestamp = statrTimestamp + interval;
//                ctx.timerService().registerEventTimeTimer(timerTimestamp);
//
//                // 加入新元素
//                reducingState.add(value.f1);
//            }
//
//            @Override
//            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
//                super.onTimer(timestamp, ctx, out);
//                out.collect("[" + ctx.getCurrentKey() + "] " + "10s内最大温度是" + reducingState.get());
//            }
//
//        }).print();
//
//        env.execute("reduceState");
//
//
//    }
//
//    private static class Max implements ReduceFunction<Integer> {
//
//        @Override
//        public Integer reduce(Integer integer, Integer t1) throws Exception {
//            return Math.max(integer,t1);
//        }
//    }
//}