package javatest01.TwoPhaseCommitTest;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author jiangfan
 * @date 2022/9/30 12:08
 */

public class MyMapper implements MapFunction<String, Tuple2<String, Integer>>
{
    @Override
    public Tuple2<String, Integer> map(String value) throws Exception {
        return new Tuple2<String,Integer>(value, 1);
    }
}
