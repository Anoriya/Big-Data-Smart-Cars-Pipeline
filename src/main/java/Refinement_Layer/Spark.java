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
                    String[] somme = first._2;
                    //Double[] moyenne = new Double[somme.length];
                    Double[] moyenne;

                    if (topic.equals("Empatica")) {
                        if (key.equals("ACC")) {
                            records.forEachRemaining(record -> {
                                try {
                                    SparkUtils.sum.call(somme, record._2, size);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            });
                            moyenne = SparkUtils.moyenne.call(somme,size);
                            ACC_Vector.addAll(Arrays.asList(moyenne));
                            System.out.println("ACC : " + ACC_Vector);

                        } else if (key.equals("IBI")) {
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

                        } else {
                            AtomicReference<Double> sum = new AtomicReference<Double>(Double.parseDouble(first._2[0]));
                            records.forEachRemaining(record -> {
                                try {
                                    sum.updateAndGet(v -> v + Double.parseDouble(record._2[0]));
                                    size.getAndSet((size.get() + 1));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            });
                            //Calculate average of all columns
                            double moyennez = sum.get() / size.get();
                            switch (key) {
                                case "BVP":
                                    BVP_Vector.add(moyennez);
                                    System.out.println("BVP : " + BVP_Vector);
                                    break;
                                case "EDA":
                                    EDA_Vector.add(moyennez);
                                    System.out.println("EDA : " + EDA_Vector);
                                    break;
                                case "HR":
                                    HR_Vector.add(moyennez);
                                    System.out.println("HR : " + HR_Vector);
                                    break;
                                case "TEMP":
                                    TEMP_Vector.add(moyennez);
                                    System.out.println("TEMP : " + TEMP_Vector);
                                    break;
                            }
                        }
                    }
                    else if(topic.equals("Zephyre")){
                        if(key.equals("BR_RR"))
                        {

                        }
                    }


                } catch (NoSuchElementException e) {
                    e.printStackTrace();
                }
            });
        });


        // Start the computation
        ssc.start();
        ssc.awaitTermination();

    }

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



