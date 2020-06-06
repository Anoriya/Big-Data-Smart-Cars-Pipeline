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
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "sparkxx");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

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
        List<String> EVENT_DATA_Vector = new ArrayList<String>();
        List<Double> AW_VECTOR = new ArrayList<Double>();
        List<String[]> CAMERA_VECTOR = new ArrayList<String[]>();
        List<Double> QUANTITY_Vector = new ArrayList<Double>();
        List<Double> CONCENTRATION_Vector = new ArrayList<Double>();

        topic_value.foreachRDD(rdd -> {
            rdd.foreachPartition(records -> {
                try {
                    //Taking the first record to check which partition we are in
                    Tuple2<String, String[]> first = records.next();
                    String key = first._1.split("-")[1];
                    String topic = first._1.split("-")[0];
                    //Number of the records, will be used for average
                    String[] somme = first._2;

//                    if (topic.equals("Empatica")) {
//                        if (key.equals("ACC")) {
//                            try {
//                            SparkUtils.process(records, ACC_Vector, somme, 0);
//                            System.out.println("ACC : " + ACC_Vector);}
//                            catch (Exception e){
//                                e.printStackTrace();
//                            }
//                        } else if (key.equals("IBI")) {
//                            //Get the value for which heartbeat duration is the longest
//                            try {
//                                SparkUtils.maxHeartbeat.call(first._2, records, IBI_Vector);
//                                System.out.println("IBI : " + IBI_Vector);
//                            } catch (Exception e) {
//                                e.printStackTrace();
//                            }
//                        } else {
//                            switch (key) {
//                                case "BVP":
//                                    try {
//                                    SparkUtils.process(records, BVP_Vector, somme, 0);
//                                    System.out.println("BVP : " + BVP_Vector);}
//                                    catch (Exception e){
//                                        e.printStackTrace();
//                                    }
//                                    break;
//                                case "EDA":
//                                    try {
//                                    SparkUtils.process(records, EDA_Vector, somme, 0);
//                                    System.out.println("EDA : " + EDA_Vector); }
//                                    catch (Exception e){
//                                        e.printStackTrace();
//                                    }
//                                    break;
//                                case "HR":
//                                    try {
//                                    SparkUtils.process(records, HR_Vector, somme, 0);
//                                    System.out.println("HR : " + HR_Vector);}
//                                    catch (Exception e){
//                                        e.printStackTrace();
//                                    }
//                                    break;
//                                case "TEMP":
//                                    try {
//                                    SparkUtils.process(records, TEMP_Vector, somme, 0);
//                                    System.out.println("TEMP : " + TEMP_Vector);}
//                                    catch (Exception e){
//                                        e.printStackTrace();
//                                    }
//                                    break;
//                            }
//                        }
//                    }
//                    else if (topic.equals("Zephyr")) {
//                        if (key.equals("BR_RR")) {
//                            try {
//                            SparkUtils.process(records, BR_RR_Vector, somme, 1);
//                            System.out.println("BR_RR : " + BR_RR_Vector);}
//                            catch (Exception e){
//                                e.printStackTrace();
//                            }
//                        } else if (key.equals("ECG")) {
//                            try {
//                            SparkUtils.process(records, ECG_Vector, somme, 1);
//                            System.out.println("ECG : " + ECG_Vector);}
//                            catch (Exception e){
//                                e.printStackTrace();
//                            }
//                        } else {
//                            try{
//                                SparkUtils.process_low_frequency(key, first, GENERAL_Vector, EVENT_DATA_Vector);
//                            }
//                            catch (Exception e){
//                                e.printStackTrace();
//                            }
//                        }
//                    }
//                    else
                        if (topic.equals("Aw")) {
                        //Because AW sensors always send one more empty column
                        String[] finalSomme = Arrays.copyOf(somme, somme.length - 1);
                        SparkUtils.process(records, AW_VECTOR, finalSomme, 8);
                        System.out.println("AW : " + AW_VECTOR);

                    }
//                    else if (topic.equals("Camera")) {
//                        SparkUtils.process_camera(first, records, CAMERA_VECTOR);
//                        System.out.println("Camera : " + CAMERA_VECTOR.size());
//                    }

//                    else if(topic.equals("AirQuality")){
//                            if(key.equals("Quantity")){
//                                try {
//                                    SparkUtils.process(records, QUANTITY_Vector, somme, 2);
//                                    System.out.println("Quantity : " + QUANTITY_Vector);}
//                                catch (Exception e){
//                                    e.printStackTrace();
//                                }
//                            }
//                            else if (key.equals("Concentration")){
//                                try {
//                                    SparkUtils.process(records, CONCENTRATION_Vector, somme, 2);
//                                    System.out.println("CONCENTRATION : " + CONCENTRATION_Vector);}
//                                catch (Exception e){
//                                    e.printStackTrace();
//                                }
//                            }
//
//                        }

                } catch (Exception e) {
                    System.out.println("EMPTYYY");
                }
            });
        });


        // Start the computation
        ssc.start();
        ssc.awaitTermination();

    }

}




