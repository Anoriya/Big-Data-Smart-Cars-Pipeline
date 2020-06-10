package Refinement_Layer;

import org.apache.spark.util.AccumulatorV2;

import java.util.ArrayList;
import java.util.List;

public class ListAccumulator extends AccumulatorV2<List<String[]>,List<String[]>> {
    List<String[]> list = new ArrayList<String[]>();

    public ListAccumulator() {
        super();
    }

    @Override
    public boolean isZero() {
        return list.isEmpty();
    }

    @Override
    public AccumulatorV2<List<String[]>, List<String[]>> copy() {
        ListAccumulator tmp = new ListAccumulator();
        tmp.list.addAll(list);
        return tmp;
    }

    @Override
    public void reset() {
        list.clear();
    }

    @Override
    public void add(List<String[]> v) {
        list.addAll(v);
    }

    @Override
    public void merge(AccumulatorV2<List<String[]>,List<String[]>> other) {
        list.addAll(other.value());
    }

    @Override
    public List<String[]> value() {
        return list;
    }
}