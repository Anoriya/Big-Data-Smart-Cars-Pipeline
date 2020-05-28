package Refinement_Layer;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.Function4;
import scala.Function;
import scala.Function1;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

public class SparkUtils implements Serializable {

    final static Function4<String[], String[], AtomicReference<Double>, Integer, Void> sum = new Function4<String[], String[], AtomicReference<Double>, Integer, Void>() {
        @Override
        public Void call(String[] somme, String[] record, AtomicReference<Double> size, Integer start) throws Exception {
            for (int i = start; i < somme.length; i++) {
                try {
                    somme[i] = String.valueOf((Double.parseDouble(somme[i]) + Double.parseDouble(record[i])));
                } catch (NullPointerException | NumberFormatException e) {
                    System.out.println(e.getMessage());
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
                    moyenne[i - start] = (Double.parseDouble(somme[i]) / size.get());
                } catch (NullPointerException | NumberFormatException e) {
                    moyenne[i - start] = 0.0;
                }
            }
            return moyenne;
        }
    };

    final static Function2<String[], Iterator<Tuple2<String, String[]>>, Tuple2<Double, Double>> maxHeartbeat = new Function2<String[], Iterator<Tuple2<String, String[]>>, Tuple2<Double, Double>>() {
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

    final static Function1<String[], Double[]> convertArrayOfStringsToDouble = new Function1<String[], Double[]>() {
        @Override
        public Double[] apply(String[] stringsArray) {
            Double[] doublesArray = new Double[stringsArray.length];
            for (int i = 0; i < doublesArray.length; i++) {
                try {
                    doublesArray[i] = Double.parseDouble(stringsArray[i]);
                }
                catch (NullPointerException | NumberFormatException e){
                    doublesArray[i] = 0.0;
                }
            }
            return doublesArray;
        }

        @Override
        public <A> Function1<A, Double[]> compose(Function1<A, String[]> function1) {
            return null;
        }

        @Override
        public <A> Function1<String[], A> andThen(Function1<Double[], A> function1) {
            return null;
        }

        @Override
        public boolean apply$mcZD$sp(double v) {
            return false;
        }

        @Override
        public double apply$mcDD$sp(double v) {
            return 0;
        }

        @Override
        public float apply$mcFD$sp(double v) {
            return 0;
        }

        @Override
        public int apply$mcID$sp(double v) {
            return 0;
        }

        @Override
        public long apply$mcJD$sp(double v) {
            return 0;
        }

        @Override
        public void apply$mcVD$sp(double v) {

        }

        @Override
        public boolean apply$mcZF$sp(float v) {
            return false;
        }

        @Override
        public double apply$mcDF$sp(float v) {
            return 0;
        }

        @Override
        public float apply$mcFF$sp(float v) {
            return 0;
        }

        @Override
        public int apply$mcIF$sp(float v) {
            return 0;
        }

        @Override
        public long apply$mcJF$sp(float v) {
            return 0;
        }

        @Override
        public void apply$mcVF$sp(float v) {

        }

        @Override
        public boolean apply$mcZI$sp(int i) {
            return false;
        }

        @Override
        public double apply$mcDI$sp(int i) {
            return 0;
        }

        @Override
        public float apply$mcFI$sp(int i) {
            return 0;
        }

        @Override
        public int apply$mcII$sp(int i) {
            return 0;
        }

        @Override
        public long apply$mcJI$sp(int i) {
            return 0;
        }

        @Override
        public void apply$mcVI$sp(int i) {

        }

        @Override
        public boolean apply$mcZJ$sp(long l) {
            return false;
        }

        @Override
        public double apply$mcDJ$sp(long l) {
            return 0;
        }

        @Override
        public float apply$mcFJ$sp(long l) {
            return 0;
        }

        @Override
        public int apply$mcIJ$sp(long l) {
            return 0;
        }

        @Override
        public long apply$mcJJ$sp(long l) {
            return 0;
        }

        @Override
        public void apply$mcVJ$sp(long l) {

        }
    };
}
