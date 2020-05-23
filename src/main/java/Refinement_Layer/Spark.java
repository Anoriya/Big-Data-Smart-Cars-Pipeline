package Refinement_Layer;


import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

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
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(5000));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "quickstart.cloudera:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "sparkxx");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("Aw", "Zephyr", "Camera", "Empatica");

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferBrokers(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        JavaPairDStream<String, String[]> topic_value = stream.mapToPair(record -> new Tuple2<>(record.topic() + "-" + record.key(), record.value().split(";")));

//        JavaPairDStream<String, Integer> topic_count = topic_pairs.reduceByKey(reduceSumFunc);

        topic_value.foreachRDD(rdd -> {
            rdd.foreachPartition(records -> {
                String key =records.next()._1.split("-")[1];
                AtomicReference<Double> sum = new AtomicReference<>((double) 0);
                AtomicReference<Double> size = new AtomicReference<>((double) 0);
                if (key.equals("camera")){
                    records.forEachRemaining(record -> {
                        String value = "0";
                        if (!record._2[8].isEmpty())
                            value = record._2[8];
                        String finalValue = value;
                        sum.updateAndGet(v -> (double) (v + Double.parseDouble(finalValue)));
                        size.getAndSet((double) (size.get() + 1));
                    });
                }
                else if (key.equals("aw")) {
                    records.forEachRemaining(record -> {
                        sum.updateAndGet(v -> (double) (v + Double.parseDouble(record._2[15])));
                        size.getAndSet((double) (size.get() + 1));
                    });
                }
                double moy = sum.get() / size.get();
                System.out.println("EL MOYENNE de " + key + " : " + moy);
            });
        });


        // Start the computation
        ssc.start();
        ssc.awaitTermination();

    }


}
