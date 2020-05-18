package Refinement_Layer;


import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;

import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;
import org.apache.spark.streaming.kafka010.*;

public class Spark {

    public Spark() {

        SparkConf conf = new SparkConf().setAppName("appName").setMaster("yarn");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(5000));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "quickstart.cloudera:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "g25");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Collections.singletonList("test");

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferBrokers(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        JavaPairDStream<String, String> result = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

        result.foreachRDD(rdd -> {
            List<Tuple2<String, String>> res = rdd.collect();
            System.out.println("SIZEEEEEEEEEE" + res.size());
        });

    }


}