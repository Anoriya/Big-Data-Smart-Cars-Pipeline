package Refinement_Layer;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;

public class SparkUtils implements Serializable {

    final static Function3<String[], String[], AtomicReference<Double>, Void> sum = new Function3<String[], String[], AtomicReference<Double>, Void>() {
        @Override
        public Void call(String[] somme, String[] record, AtomicReference<Double> size) throws Exception {
            for (int i = 0; i < somme.length; i++) {
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

    final static Function2<String[], AtomicReference<Double>, Double[]> moyenne = new Function2<String[], AtomicReference<Double>, Double[]>() {
        @Override
        public Double[] call(String[] somme, AtomicReference<Double> size) throws Exception {
            Double[] moyenne = new Double[somme.length];
            for (int i = 0; i < somme.length; i++) {
                try {
                    moyenne[i] = (Double.parseDouble(somme[i]) / size.get());
                } catch (NullPointerException | NumberFormatException e) {
                    moyenne[i] = 0.0;
                }
            }
            return moyenne;
        }
    };
}
