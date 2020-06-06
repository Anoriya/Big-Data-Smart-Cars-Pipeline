package DBSCAN;

import DBSCAN.metrics.DistanceMetricNumbers;
import org.junit.Test;

import static org.junit.Assert.*;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Random;


/**
 * Unit tests for DBSCANClusterer
 *
 * @author <a href="mailto:cf@christopherfrantz.org">Christopher Frantz</a>
 * @version 0.1
 */
public class TestDBSCAN {


    @Test
    public void testClustering() {

        Random random = new Random(4522);

        ArrayList<ArrayList<Double>> numbers = new ArrayList<ArrayList<Double>>();


        int i = 0;
        int j = 0;
            ArrayList<Double> newlist = new ArrayList<Double>();
        ArrayList<Double> newlist1 = new ArrayList<Double>();
        ArrayList<Double> newlist2 = new ArrayList<Double>();

            while (j < 8) {
                newlist.add(2000.0);
                j++;
            }
            numbers.add(newlist);
            j = 0;

        while (j < 8) {
            newlist1.add(0.0);
            j++;
        }
        numbers.add(newlist1);
        j = 0;

        while (j < 8) {
            newlist2.add(-5000.0);
            j++;
        }
        numbers.add(newlist2);


        int minCluster = 1;
        double maxDistance = 2500;

        DBSCANClusterer<ArrayList<Double>> clusterer = null;
        try {
            clusterer = new DBSCANClusterer<ArrayList<Double>>(numbers, minCluster, maxDistance, new DistanceMetricNumbers(),0);
        } catch (DBSCANClusteringException e1) {
            fail("Should not have failed on instantiation: " + e1);
        }

        ArrayList<ArrayList<Double>> result = null;

        try {
            result = clusterer.performClustering();
        } catch (DBSCANClusteringException e) {
            fail("Should not have failed while performing clustering: " + e);
        }

        System.out.println("Number of clusters " + result.size());
    }

}