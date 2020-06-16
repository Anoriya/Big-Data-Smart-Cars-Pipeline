package Refinement_Layer;


import Kafka.CarProducer;
import Refinement_Layer.Accumulators.Attributes;
import Refinement_Layer.Accumulators.CameraAccumulator;
import Refinement_Layer.Accumulators.MapAccumulator;
import org.apache.spark.streaming.api.java.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;


public class Spark {

    public Spark() throws InterruptedException {


        JavaStreamingContext ssc = SparkConfig.getStreamingContext();

        //Camera accum
        CameraAccumulator CAMERA_ACCUM = new CameraAccumulator();
        ssc.ssc().sparkContext().register(CAMERA_ACCUM, "camera");
        //Empatica accum
        MapAccumulator EMPATICA_ACCUM = new MapAccumulator();
        ssc.ssc().sparkContext().register(EMPATICA_ACCUM, "Empatica");
        //Zephyr accum
        MapAccumulator ZEPHYR_ACCUM = new MapAccumulator();
        ssc.ssc().sparkContext().register(ZEPHYR_ACCUM, "Zephyr");
        //AirQ accum
        MapAccumulator AIRQ_ACCUM = new MapAccumulator();
        ssc.ssc().sparkContext().register(AIRQ_ACCUM, "AirQuality");
        //Aw accum
        MapAccumulator AW_ACCUM = new MapAccumulator();
        ssc.ssc().sparkContext().register(AW_ACCUM, "Aw");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "quickstart.cloudera:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "sparkxx");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", true);

        Collection<String> topics = Arrays.asList("Aw", "Zephyr", "Camera", "Empatica", "AirQuality");

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferBrokers(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        JavaPairDStream<String, String[]> topic_value = stream.mapToPair(record -> new Tuple2<>(record.topic() + "-" + record.key(), record.value().split(";")));

        //        JavaPairDStream<String, Integer> topic_count = topic_pairs.reduceByKey(reduceSumFunc);
//        //Vectors that will be passed to the model
//        List<Double> ACC_Vector = new ArrayList<Double>();
//        List<Double> IBI_Vector = new ArrayList<Double>();
//        List<Double> EDA_Vector = new ArrayList<Double>();
//        List<Double> HR_Vector = new ArrayList<Double>();
//        List<Double> TEMP_Vector = new ArrayList<Double>();
//        List<Double> BVP_Vector = new ArrayList<Double>();
//        List<Double> BR_RR_Vector = new ArrayList<Double>();
//        List<Double> ECG_Vector = new ArrayList<Double>();
//        List<Double> GENERAL_Vector = new ArrayList<Double>();
//        List<Double> AW_VECTOR = new ArrayList<Double>();
//        List<Double> QUANTITY_Vector = new ArrayList<Double>();
//        List<Double> CONCENTRATION_Vector = new ArrayList<Double>();


        topic_value.foreachRDD(rdd -> {
            CAMERA_ACCUM.reset();
            AIRQ_ACCUM.reset();
            EMPATICA_ACCUM.reset();
            ZEPHYR_ACCUM.reset();
            AW_ACCUM.reset();
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
                                    EMPATICA_ACCUM.add_to_map("ACC", SparkUtils.createMap(Attributes.key_Empatica,SparkUtils.processLowFlow(records, first._2, 0)));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            } else if (key.equals("IBI")) {
                                //Get the value for which heartbeat duration is the longest
                                try {
                                    EMPATICA_ACCUM.add_to_map("IBI", SparkUtils.createMap(Attributes.key_Empatica,SparkUtils.maxHeartbeat.call(first._2, records)));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            } else {
                                switch (key) {
                                    case "BVP":
                                        try {
                                            EMPATICA_ACCUM.add_to_map("BVP", SparkUtils.createMap(Attributes.key_Empatica,SparkUtils.processLowFlow(records, first._2, 0)));
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                        break;
                                    case "EDA":
                                        try {
                                            EMPATICA_ACCUM.add_to_map("EDA", SparkUtils.createMap(Attributes.key_Empatica,SparkUtils.processLowFlow(records, first._2, 0)));
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                        break;
                                    case "HR":
                                        try {
                                            EMPATICA_ACCUM.add_to_map("HR", SparkUtils.createMap(Attributes.key_Empatica,SparkUtils.processLowFlow(records, first._2, 0)));
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                        break;
                                    case "TEMP":
                                        try {
                                            EMPATICA_ACCUM.add_to_map("TEMP", SparkUtils.createMap(Attributes.key_Empatica,SparkUtils.processLowFlow(records, first._2, 0)));
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                        break;
                                }
                            }
                            break;
                        case "Zephyr":
                            switch (key) {
                                case "BR_RR":
                                    try {
                                        String[] cleaned_First = Arrays.copyOfRange(first._2, 1, first._2.length);
                                        ZEPHYR_ACCUM.add_to_map("BR_RR", SparkUtils.createMap(Attributes.key_BR_RR, SparkUtils.processWithClustering(records, cleaned_First, 1, first._2.length, 0, (double) 30)));
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                    break;
                                case "ECG":
                                    try {
                                        String[] cleaned_First = Arrays.copyOfRange(first._2, 1, first._2.length);
                                        ZEPHYR_ACCUM.add_to_map("ECG", SparkUtils.createMap(Attributes.key_ECG, SparkUtils.processWithClustering(records, cleaned_First, 1, first._2.length, 0, (double) 10)));
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                    break;
                                case "General":
                                    try {
                                        ZEPHYR_ACCUM.add_to_map("GENERAL", SparkUtils.createMap(Attributes.key_General, SparkUtils.process_low_frequency(first._2, 1)));
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                    break;
                            }
                            break;
                        case "Aw": {
                            //Because AW sensors always send one more empty column
                            String[] cleaned_First = Arrays.copyOfRange(first._2, 8, first._2.length - 1);
                            AW_ACCUM.add_to_map("AW", SparkUtils.createMap(Attributes.key_Aw, SparkUtils.processWithClustering(records, cleaned_First, 8, first._2.length - 1, 5, (double) 200)));
                            break;
                        }
                        case "Camera":
                            CAMERA_ACCUM.add(SparkUtils.process_camera(first, records));
                            break;
                        case "AirQuality": {
                            String[] cleaned_First = Arrays.copyOfRange(first._2, 2, first._2.length);
                            if (key.equals("Quantity")) {
                                try {
                                    AIRQ_ACCUM.add_to_map("Quantity", SparkUtils.createMap(Attributes.key_AirQ_Quantity, SparkUtils.processLowFlow(records, cleaned_First, 2)));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            } else if (key.equals("Concentration")) {
                                try {
                                    AIRQ_ACCUM.add_to_map("Concentration", SparkUtils.createMap(Attributes.key_AirQ_Concentration, SparkUtils.processLowFlow(records, cleaned_First, 2)));
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
            // Save the sensors data timestamped to couchDB database
            SparkUtils.save_to_database(EMPATICA_ACCUM, ZEPHYR_ACCUM, AIRQ_ACCUM, CAMERA_ACCUM, AW_ACCUM);

            //Predict and send result
            CarProducer.sendRecord("result", Double.toString(Math.random()));

            });



        // Start the computation
        ssc.start();
        ssc.awaitTermination();

    }

}




