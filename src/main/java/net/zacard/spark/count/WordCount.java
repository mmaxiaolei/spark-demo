package net.zacard.spark.count;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author guoqw
 * @since 2017-02-15 17:21
 */
public class WordCount {

    public static void main(String[] args) {
        // 创建Spark Context
        SparkConf conf = new SparkConf().setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 读取文件数据
        String fileName = "/Users/guoqw/x/test/spark-2.1.0-bin-hadoop2.7/README.md";
        JavaRDD<String> lines = sc.textFile(fileName);
        // 切分单词
        JavaRDD<String> words = lines.flatMap(
                x -> Arrays.asList(x.split(" ")).iterator());
        // 转换为键值对并计
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                word -> new Tuple2<>(word, 1))
                .reduceByKey((x, y) -> x + y);
        // 将统计出来的单词总数存入一个文本文件，引发求值
        String outputFile = "/Users/guoqw/x/test/spark-2.1.0-bin-hadoop2.7/wordCount";
        counts.saveAsTextFile(outputFile);
    }
}
