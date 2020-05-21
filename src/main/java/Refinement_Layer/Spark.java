package Refinement_Layer;


import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;

import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;
import org.apache.spark.api.java.function.Function2;

public class Spark {

    public Spark() throws InterruptedException {

        Function2<Integer, Integer, Integer> reduceSumFunc = (key, n) -> (key + n);
        SparkConf conf = new SparkConf().setAppName("appName");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(2000));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "quickstart.cloudera:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", true);

        Collection<String> topics = Arrays.asList("Aw", "Zephyr", "Camera", "Empatica");

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferBrokers(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        JavaPairDStream<String, Integer> topic_pairs = stream.mapToPair(record -> new Tuple2<>(record.topic(), 1));

        JavaPairDStream<String, Integer> topic_count = topic_pairs.reduceByKey(reduceSumFunc);

            topic_count.foreachRDD(rdd -> {
                List<Tuple2<String, Integer>> sample = rdd.collect();
                sample.forEach(System.out::println);
        });



        // Start the computation
        ssc.start();
        ssc.awaitTermination();

    }


}
