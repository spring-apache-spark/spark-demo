package com.nsc.sparkdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import scala.Tuple2;

import javax.annotation.PostConstruct;
import java.util.Arrays;

@SpringBootApplication
public class SparkDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(SparkDemoApplication.class, args);
    }

    @PostConstruct
    public void sparkTest() {
        SparkConf sparkConf = new SparkConf().setAppName("Spark World Count").setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile("C:\\Users\\212772767\\Desktop\\words.txt");
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        JavaPairRDD<String, Integer> couples = words.mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairRDD<String, Integer> result = couples.reduceByKey(Integer::sum);

        result.foreach(res -> {
            System.out.println(res._1() + " " + res._2());
        });
    }
}
