package Refinement_Layer;

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
}
