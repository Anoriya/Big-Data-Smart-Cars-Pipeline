import Refinement_Layer.Spark;
import scala.Tuple2;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class main {
    public static void main(String[] args) throws InterruptedException, IOException {


//        Spark spark = new Spark();
        AtomicReference<Tuple2<Double, Double>> max = new AtomicReference<>(new Tuple2<>(Double.parseDouble("33"), Double.parseDouble("42.5")));
        System.out.println(max);
    }
}
