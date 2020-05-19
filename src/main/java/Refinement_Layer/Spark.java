package Refinement_Layer;


import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;

import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.kafka010.*;

public class Spark {

    public Spark() throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("appName").setMaster("yarn");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(5000));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "quickstart.cloudera:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "g25");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("test", "camera");

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        JavaDStream<String> lines = messages.map(ConsumerRecord::value);
        lines.print();

        // Start the computation
        ssc.start();
        ssc.awaitTermination();

    }


}