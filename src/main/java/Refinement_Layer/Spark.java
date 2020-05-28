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
        List<Double> BR_RR_Vector = new ArrayList<Double>();
        List<Double> ECG_Vector = new ArrayList<Double>();
        List<Double> GENERAL_Vector = new ArrayList<Double>();
        List<String> EVEN_DATA_Vector = new ArrayList<String>();
        List<Double> AW_VECTOR = new ArrayList<Double>();
        List<String[]> CAMERA_VECTOR = new ArrayList<String[]>();

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
                    Double[] moyenne;

                    if (topic.equals("Empatica")) {
                        if (key.equals("ACC")) {
                            records.forEachRemaining(record -> {
                                try {
                                    SparkUtils.sum.call(somme, record._2, size,0);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            });
                            moyenne = SparkUtils.moyenne.call(somme,size,0);
                            ACC_Vector.addAll(Arrays.asList(moyenne));
                            System.out.println("ACC : " + ACC_Vector);

                        } else if (key.equals("IBI")) {
                            //Get the value for which heartbeat duration is the longest
                            try {
                                Tuple2<Double, Double> max = SparkUtils.maxHeartbeat.call(first._2, records);
                                IBI_Vector.addAll(Arrays.asList(max._1, max._2));
                            } catch (Exception e) {
                                IBI_Vector.addAll(Arrays.asList(0.0, 0.0));
                            }
                            System.out.println("IBI : " + IBI_Vector);

                        } else {
                            records.forEachRemaining(record -> {
                                try {
                                    SparkUtils.sum.call(somme, record._2, size,0);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            });
                            //Calculate average of all columns
                            moyenne = SparkUtils.moyenne.call(somme,size,0);
                            switch (key) {
                                case "BVP":
                                    BVP_Vector.addAll(Arrays.asList(moyenne));
                                    System.out.println("BVP : " + BVP_Vector);
                                    break;
                                case "EDA":
                                    EDA_Vector.addAll(Arrays.asList(moyenne));
                                    System.out.println("EDA : " + EDA_Vector);
                                    break;
                                case "HR":
                                    HR_Vector.addAll(Arrays.asList(moyenne));
                                    System.out.println("HR : " + HR_Vector);
                                    break;
                                case "TEMP":
                                    TEMP_Vector.addAll(Arrays.asList(moyenne));
                                    System.out.println("TEMP : " + TEMP_Vector);
                                    break;
                            }
                        }
                    }

                    else if(topic.equals("Zephyr")){
                        if(key.equals("BR_RR"))
                        {
                            records.forEachRemaining(record -> {
                                try {
                                    SparkUtils.sum.call(somme, record._2, size,1);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            });
                            moyenne = SparkUtils.moyenne.call(somme,size,1);
                            BR_RR_Vector.addAll(Arrays.asList(moyenne));
                            System.out.println("BR_RR : " + BR_RR_Vector);
                        }

                    else if(key.equals("ECG"))
                    {
                        records.forEachRemaining(record -> {
                            try {
                                SparkUtils.sum.call(somme, record._2, size,1);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
                        moyenne = SparkUtils.moyenne.call(somme,size,1);
                        ECG_Vector.addAll(Arrays.asList(moyenne));
                        System.out.println("ECG : " + ECG_Vector);
                    }
                    else if(key.equals("General"))
                    {
                        records.forEachRemaining(record -> {
                                //Removes the timestamp attribute
                                String[] nonTimedRecord = Arrays.copyOfRange(record._2, 1, record._2.length);
                                Double[] convertedArray = SparkUtils.convertArrayOfStringsToDouble.apply(nonTimedRecord);
                                GENERAL_Vector.addAll(Arrays.asList(convertedArray));
                        });
                        System.out.println("GENERAL : " + GENERAL_Vector);
                    }
                        else if(key.equals("Event_Data"))
                        {
                            records.forEachRemaining(record -> {
                                try{
                                String [] newRecord = Arrays.copyOfRange(record._2, 5, record._2.length);
                                EVEN_DATA_Vector.addAll(Arrays.asList(newRecord));}
                                catch(Exception e){
                                    e.printStackTrace();
                                }
                            });
                            System.out.println("Event Data : " + EVEN_DATA_Vector);
                        }
                }
                     else if(topic.equals("Aw"))
                    {
                        //Because AW sensors always send one more empty column
                        String[] finalSomme = Arrays.copyOf(somme, somme.length - 1);
                        records.forEachRemaining(record -> {
                            try {
                                SparkUtils.sum.call(finalSomme, record._2, size,8);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
                        moyenne = SparkUtils.moyenne.call(finalSomme,size,8);
                        AW_VECTOR.addAll(Arrays.asList(moyenne));
                        System.out.println("AW : " + AW_VECTOR);
                    }
                    else if(topic.equals("Camera"))
                    {
                        //Because AW sensors always send one more empty column
                        records.forEachRemaining(record -> {
                            try {
                                CAMERA_VECTOR.add(record._2);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
                        System.out.println("Camera : " + CAMERA_VECTOR);
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




