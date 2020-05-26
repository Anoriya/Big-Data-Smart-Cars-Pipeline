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

public class Spark {

    public Spark() throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("appName");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(2000));

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
        //Vectors that will be passed to the model
        List<Double> ACC_Vector = new ArrayList<Double>();
        List<Double> IBI_Vector = new ArrayList<Double>();
        List<Double> EDA_Vector = new ArrayList<Double>();
        List<Double> HR_Vector = new ArrayList<Double>();
        List<Double> TEMP_Vector = new ArrayList<Double>();
        List<Double> BVP_Vector = new ArrayList<Double>();


        topic_value.foreachRDD(rdd -> {
            rdd.foreachPartition(records -> {
                try {
                    //Taking the first record to check which partition we are in
                    Tuple2<String, String[]> first = records.next();
                    String key = first._1.split("-")[1];
                    String topic = first._1.split("-")[0];
                    //Number of the records, will be used for average
                    AtomicReference<Double> size = new AtomicReference<>((double) 1);

                    if (topic.equals("Empatica")) {
                        if (key.equals("ACC")) {
                            try {
                                AtomicReference<Double> col1 = new AtomicReference<>(Double.parseDouble(first._2[0]));
                                AtomicReference<Double> col2 = new AtomicReference<>(Double.parseDouble(first._2[1]));
                                AtomicReference<Double> col3 = new AtomicReference<>(Double.parseDouble(first._2[2]));
                                records.forEachRemaining(record -> {
                                    col1.updateAndGet(v -> v + Double.parseDouble(record._2[0]));
                                    col2.updateAndGet(v -> v + Double.parseDouble(record._2[1]));
                                    col3.updateAndGet(v -> v + Double.parseDouble(record._2[2]));
                                    size.getAndSet((size.get() + 1));
                                });
                                double moy1 = col1.get() / size.get();
                                double moy2 = col2.get() / size.get();
                                double moy3 = col3.get() / size.get();
                                ACC_Vector.addAll(Arrays.asList(moy1, moy2, moy3));
                            } catch (Exception e) {
                                ACC_Vector.addAll(Arrays.asList(0.0, 0.0, 0.0));
                            }
                            System.out.println("ACC : " + ACC_Vector);

                        } 
                        else if (key.equals("IBI")) {
                            //Get the value for which heartbeat duration is the longest
                            try {
                                AtomicReference<Tuple2<Double, Double>> max = new AtomicReference<>(new Tuple2<>(Double.parseDouble(first._2[0]), Double.parseDouble(first._2[1])));
                                records.forEachRemaining(record -> {
                                    if (Double.parseDouble(record._2[1]) > max.get()._2) {
                                        max.set(new Tuple2<>(Double.parseDouble(record._2[0]), Double.parseDouble(record._2[1])));
                                    }
                                });
                                IBI_Vector.addAll(Arrays.asList(max.get()._1, max.get()._2));
                            } catch (Exception e) {
                                IBI_Vector.addAll(Arrays.asList(0.0, 0.0));
                            }
                            System.out.println("IBI : " + IBI_Vector);

                        }
                        else {
                            AtomicReference<Double> sum = new AtomicReference<Double>(Double.parseDouble(first._2[0]));
                            records.forEachRemaining(record -> {
                                try {
                                    sum.updateAndGet(v -> v + Double.parseDouble(record._2[0]));
                                    size.getAndSet((size.get() + 1));
                                }
                                catch (Exception e){
                                    e.printStackTrace();
                                }
                            });
                            //Calculate average of all columns
                            double moyenne = sum.get() / size.get();
                            switch (key) {
                                case "BVP":
                                    BVP_Vector.add(moyenne);
                                    System.out.println("BVP : " + BVP_Vector);
                                    break;
                                case "EDA":
                                    EDA_Vector.add(moyenne);
                                    System.out.println("EDA : " + EDA_Vector);
                                    break;
                                case "HR":
                                    HR_Vector.add(moyenne);
                                    System.out.println("HR : " + HR_Vector);
                                    break;
                                case "TEMP":
                                    TEMP_Vector.add(moyenne);
                                    System.out.println("TEMP : " + TEMP_Vector);
                                    break;
                            }
                        }
                    }


                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        });


        // Start the computation
        ssc.start();
        ssc.awaitTermination();
    }

//        private Tuple2<Double, Double> getMaximumHeartbeat (String[]first, Iterator < Tuple2 < String, String[]>>records) throws
//        Exception {
//            AtomicReference<Tuple2<Double, Double>> max = new AtomicReference<>(new Tuple2<>(Double.parseDouble(first[0]), Double.parseDouble(first[1])));
//            records.forEachRemaining(record -> {
//                if (Double.parseDouble(record._2[1]) > max.get()._2) {
//                    max.set(new Tuple2<>(Double.parseDouble(record._2[0]), Double.parseDouble(record._2[1])));
//                }
//            });
//            return max.get();
//        }

    }

