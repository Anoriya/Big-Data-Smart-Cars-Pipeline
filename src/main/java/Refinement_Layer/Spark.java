package Refinement_Layer;


import Refinement_Layer.Accumulators.CameraAccumulator;
import Refinement_Layer.Accumulators.ListAccumulator;
import Refinement_Layer.Accumulators.MapAccumulator;
import org.apache.spark.streaming.api.java.*;

import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;


public class Spark {

    public Map<String,Double> CreateMap(String[] key,List<Double> value){
        Map<String,Double> map = null;
        for (int i=0; i<value.size();i++){
            map.put(key[i],value.get(i));
        }
        return map;
    }

    public Spark() throws InterruptedException {

        String[] key_AirQ_Quantity = new String[]{"SOUT_ETOH_SGP30_8373181","SOUT_H2_SGP30_8373181","C_CO2eq_SGP30_8373181","C_TVOC_SGP30_8373181"};
        String[] key_AirQ_Concentration = new String[]{"MassConc_1p0_SPS3x_ECAC46730C998045","MassConc_2p5_SPS3x_ECAC46730C998045","MassConc_4p0_SPS3x_ECAC46730C998045","MassConc_10p_SPS3x_ECAC46730C998045","NumbConc_0p5_SPS3x_ECAC46730C998045","NumbConc_1p0_SPS3x_ECAC46730C998045","NumbConc_2p5_SPS3x_ECAC46730C998045","NumbConc_2p5_SPS3x_ECAC46730C998045","NumbConc_10p_SPS3x_ECAC46730C998045","TypPartSize_SPS3x_ECAC46730C998045"};
        String[] key_Aw = new String[]{"Engine_Load_[%]","Engine_Speed_[RPM]","Vehicule_Speed_[km/h]","fHR","fRR","Sensor_1","Sensor_2","Sensor_3","Sensor_4","Sensor_5","Sensor_6","Sensor_7","Sensor_8"};
        String[] key_Camera = new String[]{"head_pose_yaw","head_pose_pitch	head_pose_roll","head_pose_yaw_cal","head_pose_pitch_cal","head_pose_roll_cal",	"pupil_dilation_ratio",	"right_eye_open_perc",	"left_eye_open_perc	right_eye_open_mm",	"left_eye_open_mm",	"R_eye_x","R_eye_y","R_eye_z","R_eye_x_cal","R_eye_y_cal","R_eye_z_cal","R_eye_gaze_yaw","R_eye_gaze_pitch","R_eye_gaze_yaw_cal",	"R_eye_gaze_pitch_cal",	"L_eye_x",	"L_eye_y",	"L_eye_z",	"L_eye_x_cal",	"L_eye_y_cal",	"L_eye_z_cal",	"L_eye_gaze_yaw",	"L_eye_gaze_pitch",	"L_eye_gaze_yaw_cal",	"L_eye_gaze_pitch_cal",	"gaze_yaw",	"gaze_pitch",	"gaze_yaw_cal",	"gaze_pitch_cal",	"gaze_x",	"gaze_y",	"gaze_z",	"gaze_x_cal",	"gaze_y_cal",	",gaze_z_cal",	"head_coord_x",	"head_coord_y",	"head_coord_z",	"head_coord_x_cal",	"head_coord_y_cal",	"head_coord_z_cal"	,"id",	"user_name",	"is_smoking",	"is_using_phone",	"is_wearing_seatbelt","has_glasses","eyes_on_road","attentiveness","drowsiness","aoi","aoi_x","aoi_y","aoi_z"};
        String[] key_BR_RR = new String[]{"BR","RtoR"};
        String[] key_ECG = new String[]{"ECG"};
        String[] key_General = new String[]{"HR",	"BR",	"Temp",	"Posture",	"Activity",	"Acceleration",	"Battery",	"BRAmplitude",	"ECGAmplitude",	"ECGNoise",	"XMin",	"XPeak",	"YMin",	"YPeak",	"ZMin",	"ZPeak"};



        JavaStreamingContext ssc = SparkConfig.getStreamingContext();

        //Camera accum
        CameraAccumulator CAMERA_ACCUM = new CameraAccumulator();
        ssc.ssc().sparkContext().register(CAMERA_ACCUM, "camera");
        //Empatica accum
        ListAccumulator EMPATICA_ACCUM = new ListAccumulator();
        ssc.ssc().sparkContext().register(EMPATICA_ACCUM, "Empatica");
        //Zephyr accum
        MapAccumulator ZEPHYR_ACCUM = new MapAccumulator();
        ssc.ssc().sparkContext().register(ZEPHYR_ACCUM, "Zephyr");
        //AirQ accum
        ListAccumulator AIRQ_ACCUM = new ListAccumulator();
        ssc.ssc().sparkContext().register(AIRQ_ACCUM, "AirQuality");
        //Aw accum
        ListAccumulator AW_ACCUM = new ListAccumulator();
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
                                    EMPATICA_ACCUM.add_to_map("ACC", SparkUtils.processLowFlow(records, first._2, 0));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            } else if (key.equals("IBI")) {
                                //Get the value for which heartbeat duration is the longest
                                try {
                                    EMPATICA_ACCUM.add_to_map("IBI", SparkUtils.maxHeartbeat.call(first._2, records));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            } else {
                                switch (key) {
                                    case "BVP":
                                        try {
                                            EMPATICA_ACCUM.add_to_map("BVP", SparkUtils.processLowFlow(records, first._2, 0));
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                        break;
                                    case "EDA":
                                        try {
                                            EMPATICA_ACCUM.add_to_map("EDA", SparkUtils.processLowFlow(records, first._2, 0));
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                        break;
                                    case "HR":
                                        try {
                                            EMPATICA_ACCUM.add_to_map("HR", SparkUtils.processLowFlow(records, first._2, 0));
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                        break;
                                    case "TEMP":
                                        try {
                                            EMPATICA_ACCUM.add_to_map("TEMP", SparkUtils.processLowFlow(records, first._2, 0));
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
                                    ZEPHYR_ACCUM.add_to_map("BR_RR",CreateMap(key_BR_RR,SparkUtils.processWithClustering(records, cleaned_First, 1, first._2.length, 0, (double) 30)));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            } else if (key.equals("ECG")) {
                                try {
                                    String[] cleaned_First = Arrays.copyOfRange(first._2, 1, first._2.length);
                                    ZEPHYR_ACCUM.add_to_map("ECG",CreateMap(key_ECG,SparkUtils.processWithClustering(records, cleaned_First, 1, first._2.length, 0, (double) 10)));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            } else {
                                try {
                                    ZEPHYR_ACCUM.add_to_map("GENERAL",CreateMap(key_General,SparkUtils.process_low_frequency(first._2, 1)));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                            break;
//                        case "Aw": {
//                            //Because AW sensors always send one more empty column
//                            String[] cleaned_First = Arrays.copyOfRange(first._2, 8, first._2.length - 1);
//                            SparkUtils.processWithClustering(records, AW_VECTOR, cleaned_First, 8, first._2.length - 1, 5, (double) 200);
//                            break;
//                        }
//                        case "Camera":
//                            CAMERA_VECTOR.add(SparkUtils.process_camera(first, records));
//                            break;
//                        case "AirQuality": {
//                            String[] cleaned_First = Arrays.copyOfRange(first._2, 2, first._2.length);
//                            if (key.equals("Quantity")) {
//                                try {
//                                    SparkUtils.processLowFlow(records, QUANTITY_Vector, cleaned_First, 2);
//                                } catch (Exception e) {
//                                    e.printStackTrace();
//                                }
//                            } else if (key.equals("Concentration")) {
//                                try {
//                                    SparkUtils.processLowFlow(records, CONCENTRATION_Vector, cleaned_First, 2);
//                                } catch (Exception e) {
//                                    e.printStackTrace();
//                                }
//                            }
//
//                            break;
//                        }
                    }

                } catch (Exception e) {
                    System.out.println("EMPTYYY");
                }
            });
            System.out.println("EMPATICA : " + EMPATICA_ACCUM.value());
            try {
            CouchDB.createDocument(EMPATICA_ACCUM.value()); }
            catch (Exception e){
                System.out.println("Ta7che");
            }
        });


        // Start the computation
        ssc.start();
        ssc.awaitTermination();

    }

}




