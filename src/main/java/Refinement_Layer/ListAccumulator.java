package Refinement_Layer;

import org.apache.spark.util.AccumulatorV2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ListAccumulator extends AccumulatorV2<List<Double[]>,Map<String, Object>> {
    Map<String, Object> map = new HashMap<String, Object>();

    public ListAccumulator() {
        super();
    }

    @Override
    public boolean isZero() {
        return map.isEmpty();
    }

    @Override
    public AccumulatorV2<List<Double[]>, Map<String, Object>> copy() {
        return new ListAccumulator();
    }

    @Override
    public void reset() {
        map.clear();
    }

    @Override
    public void add(List<Double[]> v) {
            }
    
    public void add_to_map(String key, List<Double> vector){
        map.put(key, vector);
    }

    @Override
    public void merge(AccumulatorV2<List<Double[]>, Map<String, Object>> other) {
        this.map.putAll(other.value());
    }

    @Override
    public Map<String, Object> value() {
        return map;
    }
}