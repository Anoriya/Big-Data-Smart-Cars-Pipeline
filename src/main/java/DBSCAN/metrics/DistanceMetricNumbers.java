package DBSCAN.metrics;


import DBSCAN.DBSCANClusteringException;
import DBSCAN.DistanceMetric;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Distance metric implementation for numeric values.
 */
public class DistanceMetricNumbers implements DistanceMetric<ArrayList<Double>> {

    @Override
    public double calculateDistance(int start, ArrayList<Double> val1, ArrayList<Double> val2) throws DBSCANClusteringException {
        double Manhatan_distance = 0;
        for (int i = start; i < val1.size(); i++) {
            try {
                Manhatan_distance += Math.pow(val1.get(i) - val2.get(i), 2);
            } catch (Exception e) {
                Manhatan_distance += 0;
            }
        }
//        System.out.println("DST " + Math.sqrt(Manhatan_distance)) ;
            return Math.sqrt(Manhatan_distance);
        }
    }