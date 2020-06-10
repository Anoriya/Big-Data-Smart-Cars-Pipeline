package Refinement_Layer.Accumulators;

import org.apache.spark.util.AccumulatorV2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapAccumulator  extends AccumulatorV2<Map<String, Double>, Map<String, Map<String, Double>>> {
    Map<String, Map<String, Double>> map = new HashMap<String, Map<String, Double>>();

    public MapAccumulator() {
        super();
    }

    @Override
    public boolean isZero() {
        return map.isEmpty();
    }

    @Override
    public AccumulatorV2<Map<String, Double>, Map<String, Map<String, Double>>> copy() {
        return new MapAccumulator();
    }

    @Override
    public void reset() {
        map.clear();
    }

    @Override
    public void add(Map<String, Double> v) {
    }

    public void add_to_map(String key, Map<String, Double> vector){
        map.put(key, vector);
    }

    @Override
    public void merge(AccumulatorV2<Map<String, Double>, Map<String, Map<String, Double>>> other) {
        this.map.putAll(other.value());
    }

    @Override
    public Map<String, Map<String, Double>> value() {
        return map;
    }
}