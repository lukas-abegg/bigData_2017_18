import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class Bucket extends Tuple2<Integer, ArrayList<Integer>> {

    public Bucket() {
    }

    public Bucket(Integer bid, ArrayList<Integer> items) {
        super(bid, items);
    }

    public Integer getBid() {
        return this.f0;
    }

    public ArrayList<Integer> getItems() {
        return this.f1;
    }
}