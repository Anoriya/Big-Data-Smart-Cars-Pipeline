package DBSCAN;

import java.io.Serializable;

/**
 * Interface for the implementation of distance metrics.
 * @param <V> Value type to which distance metric is applied.
 */
public interface DistanceMetric<V> extends Serializable {

    public double calculateDistance(int start, V val1, V val2) throws DBSCANClusteringException;

}