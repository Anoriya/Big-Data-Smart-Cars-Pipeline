package Refinement_Layer;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.Function4;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

public class SparkUtils implements Serializable {

    final static Function4<String[], String[], AtomicReference<Double>, Integer, Void> sum = new Function4<String[], String[], AtomicReference<Double>, Integer , Void>() {
        @Override
        public Void call(String[] somme, String[] record, AtomicReference<Double> size, Integer start) throws Exception {
            for (int i = start; i < somme.length; i++) {
                try {
                    somme[i] = String.valueOf((Double.parseDouble(somme[i]) + Double.parseDouble(record[i])));
                } catch (NullPointerException | NumberFormatException e) {
                    e.printStackTrace();
                }
            }
            size.getAndSet((size.get() + 1));
            return null;
        }
    };

    final static Function3<String[], AtomicReference<Double>, Integer, Double[]> moyenne = new Function3<String[], AtomicReference<Double>, Integer, Double[]>() {
        @Override
        public Double[] call(String[] somme, AtomicReference<Double> size, Integer start) throws Exception {
            Double[] moyenne = new Double[somme.length - start];
            for (int i = start; i < somme.length; i++) {
                try {
                    moyenne[i-start] = (Double.parseDouble(somme[i]) / size.get());
                } catch (NullPointerException | NumberFormatException e) {
                    moyenne[i-start] = 0.0;
                }
            }
            return moyenne;
        }
    };

    final static Function2<String[], Iterator<Tuple2< String, String[]>>,Tuple2<Double,Double>> maxHeartbeat = new Function2<String[], Iterator<Tuple2<String, String[]>>, Tuple2<Double, Double>>() {
        @Override
        public Tuple2<Double, Double> call(String[] first, Iterator<Tuple2<String, String[]>> records) throws Exception {
            AtomicReference<Tuple2<Double, Double>> max = new AtomicReference<>(new Tuple2<>(Double.parseDouble(first[0]), Double.parseDouble(first[1])));
            records.forEachRemaining(record -> {
                if (Double.parseDouble(record._2[1]) > max.get()._2) {
                    max.set(new Tuple2<>(Double.parseDouble(record._2[0]), Double.parseDouble(record._2[1])));
                }
            });
            return max.get();
        }
    };
}
