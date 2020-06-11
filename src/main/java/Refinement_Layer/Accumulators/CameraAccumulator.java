package Refinement_Layer.Accumulators;

import org.apache.spark.util.AccumulatorV2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static Refinement_Layer.Accumulators.Attributes.key_Camera;
import static Refinement_Layer.SparkUtils.createMapString;

public class CameraAccumulator extends AccumulatorV2<List<String[]>,List<Map<String,String>>> {
    List<Map<String,String>> list = new ArrayList<Map<String,String>>();

    public CameraAccumulator() {
        super();
    }

    @Override
    public boolean isZero() {
        return list.isEmpty();
    }

    @Override
    public AccumulatorV2<List<String[]>, List<Map<String,String>>> copy() {
        CameraAccumulator tmp = new CameraAccumulator();
        tmp.list.addAll(list);
        return tmp;
    }

    @Override
    public void reset() {
        list.clear();
    }

    @Override
    public void add(List<String[]> v) {
        for (int i=0;i<v.size();i++){
            list.add(createMapString(key_Camera,v.get(i)));
        }
    }

    @Override
    public void merge(AccumulatorV2<List<String[]>,List<Map<String,String>>> other) {
        list.addAll(other.value());
    }

    @Override
    public List<Map<String,String>> value() {
        return list;
    }
}