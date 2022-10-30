package javatest01;

/**
 * @author jiangfan
 * @date 2022/8/12 15:58
 */
public class SinkSumModel {
    public String level1="";
    public String level2="";
    public KafkaModel kafkastr=null;

    public SinkSumModel(String level1, String level2, KafkaModel kafkastr) {
        this.level1 = level1;
        this.level2 = level2;
        this.kafkastr = kafkastr;
    }
}
