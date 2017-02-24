package net.zacard.spark.steaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

import java.nio.charset.StandardCharsets;

/**
 * @author guoqw
 * @since 2017-02-22 10:08
 */
public class PringFromFlume {

    public static void main(String[] args) {
        String host = "127.0.0.1";
        // 创建Spark Config
        SparkConf conf = new SparkConf().setAppName("print-error");
        // 创建Streaming Context
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
        // 从spark-flume池中获取数据
        JavaReceiverInputDStream<SparkFlumeEvent> events = FlumeUtils.createPollingStream(jsc, host, 8881);
        // 打印event
        JavaDStream<String> contexts = events.map(event ->
                new String(event.event().getBody().array(), StandardCharsets.UTF_8));
//        contexts.persist();
//        contexts.print();
        contexts.count().print();

        // 拆成键值对的形式
/*        JavaPairDStream<Text, Text> javaPairDStream = contexts.mapToPair(context -> {
            String[] split = context.split(":");
            return new Tuple2<>(new Text(split[0]), new Text(split[1]));
        });

        class SaveOutFormat extends SequenceFileOutputFormat<Text, Text> {
        }
        // 保存结果到文件
        javaPairDStream.saveAsHadoopFiles("/Users/guoqw/x/test/spark-2.1.0-bin-hadoop2.7/flumedata",
                "txt",
                Text.class,
                Text.class,
                SaveOutFormat.class);*/

        // 启动流式计算环境，并等待它完成
        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
