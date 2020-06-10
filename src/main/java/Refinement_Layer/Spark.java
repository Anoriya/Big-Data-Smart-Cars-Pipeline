package Refinement_Layer;


import org.apache.spark.streaming.api.java.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;


public class Spark {

    public Spark() throws InterruptedException {

        JavaStreamingContext ssc = SparkConfig.getStreamingContext();

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "quickstart.cloudera:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "sparkxx");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", true);

        Collection<String> topics = Arrays.asList("Aw", "Zephyr", "Camera", "Empatica","AirQuality");

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferBrokers(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        JavaPairDStream<String, String[]> topic_value = stream.mapToPair(record -> new Tuple2<>(record.topic() + "-" + record.key(), record.value().split(";")));

        //        JavaPairDStream<String, Integer> topic_count = topic_pairs.reduceByKey(reduceSumFunc);
        //Vectors that will be passed to the model
        List<Double> ACC_Vector = new ArrayList<Double>();
        List<Double> IBI_Vector = new ArrayList<Double>();
        List<Double> EDA_Vector = new ArrayList<Double>();
        List<Double> HR_Vector = new ArrayList<Double>();
        List<Double> TEMP_Vector = new ArrayList<Double>();
        List<Double> BVP_Vector = new ArrayList<Double>();
        List<Double> BR_RR_Vector = new ArrayList<Double>();
        List<Double> ECG_Vector = new ArrayList<Double>();
        List<Double> GENERAL_Vector = new ArrayList<Double>();
        List<Double> AW_VECTOR = new ArrayList<Double>();
        AtomicReference<List<String[]>> CAMERA_VECTOR = new AtomicReference<List<String[]>>();
        List<Double> QUANTITY_Vector = new ArrayList<Double>();
        List<Double> CONCENTRATION_Vector = new ArrayList<Double>();

        topic_value.foreachRDD(rdd -> {
            rdd.foreachPartition(records -> {
                try {
                    //Taking the first record to check which partition we are in
                    Tuple2<String, String[]> first = records.next();
                    String key = first._1.split("-")[1];
                    String topic = first._1.split("-")[0];

                    switch (topic) {
                        case "Empatica":
                            if (key.equals("ACC")) {
                                try {
                                    SparkUtils.processLowFlow(records, ACC_Vector, first._2, 0);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            } else if (key.equals("IBI")) {
                                //Get the value for which heartbeat duration is the longest
                                try {
                                    SparkUtils.maxHeartbeat.call(first._2, records, IBI_Vector);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            } else {
                                switch (key) {
                                    case "BVP":
                                        try {
                                            SparkUtils.processLowFlow(records, BVP_Vector, first._2, 0);
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                        break;
                                    case "EDA":
                                        try {
                                            SparkUtils.processLowFlow(records, EDA_Vector, first._2, 0);
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                        break;
                                    case "HR":
                                        try {
                                            SparkUtils.processLowFlow(records, HR_Vector, first._2, 0);
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                        break;
                                    case "TEMP":
                                        try {
                                            SparkUtils.processLowFlow(records, TEMP_Vector, first._2, 0);
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                        break;
                                }
                            }
                            break;
                        case "Zephyr":
                            if (key.equals("BR_RR")) {
                                try {
                                    String[] cleaned_First = Arrays.copyOfRange(first._2, 1, first._2.length);
                                    SparkUtils.processWithClustering(records, BR_RR_Vector, cleaned_First, 1, first._2.length, 0, (double) 30);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            } else if (key.equals("ECG")) {
                                try {
                                    String[] cleaned_First = Arrays.copyOfRange(first._2, 1, first._2.length);
                                    SparkUtils.processWithClustering(records, ECG_Vector, cleaned_First, 1, first._2.length, 0, (double) 10);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            } else {
                                try {
                                    SparkUtils.process_low_frequency(first._2, 1, GENERAL_Vector);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                            break;
                        case "Aw": {
                            //Because AW sensors always send one more empty column
                            String[] cleaned_First = Arrays.copyOfRange(first._2, 8, first._2.length - 1);
                            SparkUtils.processWithClustering(records, AW_VECTOR, cleaned_First, 8, first._2.length - 1, 5, (double) 200);
                            break;
                        }
                        case "Camera":
                             CAMERA_VECTOR.set(SparkUtils.process_camera(first, records));
                            break;
                        case "AirQuality": {
                            String[] cleaned_First = Arrays.copyOfRange(first._2, 2, first._2.length);
                            if (key.equals("Quantity")) {
                                try {
                                    SparkUtils.processLowFlow(records, QUANTITY_Vector, cleaned_First, 2);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            } else if (key.equals("Concentration")) {
                                try {
                                    SparkUtils.processLowFlow(records, CONCENTRATION_Vector, cleaned_First, 2);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }

                            break;
                        }
                    }

                } catch (Exception e) {
                    System.out.println("EMPTYYY");
                }
            });
            System.out.println("ACC : " + ACC_Vector);
            System.out.println("IBI : " + IBI_Vector);
            System.out.println("BVP : " + BVP_Vector);
            System.out.println("EDA : " + EDA_Vector);
            System.out.println("HR : " + HR_Vector);
            System.out.println("TEMP : " + TEMP_Vector);
            System.out.println("BR_RR : " + BR_RR_Vector);
            System.out.println("ECG : " + ECG_Vector);
            System.out.println("GENERAL : " + GENERAL_Vector);
            System.out.println("AW : " + AW_VECTOR);
            System.out.println("Camera : " + CAMERA_VECTOR.get().size());
            System.out.println("Quantity : " + QUANTITY_Vector);
            System.out.println("CONCENTRATION : " + CONCENTRATION_Vector);
        });




        // Start the computation
        ssc.start();
        ssc.awaitTermination();

    }

}




