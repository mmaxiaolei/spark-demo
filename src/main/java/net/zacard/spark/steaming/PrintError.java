package net.zacard.spark.steaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author guoqw
 * @since 2017-02-16 14:23
 */
public class PrintError {

    public static void main(String[] args) {
        // 创建Spark Config
        SparkConf conf = new SparkConf().setAppName("print-error");
        // 创建Streaming Context
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));
        // 以端口7777作为输入源创建Dstream
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("127.0.0.1", 7777);
        // 从Dstream中筛选出包含"error"的行
        JavaDStream<String> errorLines = lines.filter(line -> line.contains("error"));
        // 打印出有"error"的行
        errorLines.print();

        // 启动流式计算环境，并等待它完成
        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
