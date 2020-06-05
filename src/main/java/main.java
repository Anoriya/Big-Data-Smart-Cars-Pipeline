import DBSCAN.TestDBSCAN;
import Refinement_Layer.Spark;

import java.io.IOException;
import java.util.ArrayList;

public class main {
    public static void main(String[] args) throws InterruptedException, IOException {


//        Spark spark = new Spark();
        TestDBSCAN test = new TestDBSCAN();
        test.testClustering();
    }
}
