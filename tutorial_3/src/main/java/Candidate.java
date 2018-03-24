import org.apache.flink.api.java.tuple.Tuple5;

import java.io.Serializable;
import java.util.ArrayList;

public class Candidate extends Tuple5<String, String, ArrayList<Integer>, Integer, Integer> implements Serializable {

    public Candidate() {
    }

    public Candidate(String key, String parent, ArrayList<Integer> items, Integer frequency, Integer level) {
        super(key, parent, items, frequency, level);
    }

    public String getKey() {
        return this.f0;
    }

    public String getParent() {
        return this.f1;
    }

    public ArrayList<Integer> getItems() {
        return this.f2;
    }

    public Integer getFrequency() {
        return this.f3;
    }

    public Integer getLevel() {
        return this.f4;
    }
}