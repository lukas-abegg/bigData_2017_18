import org.apache.flink.api.java.tuple.Tuple4;

import java.io.Serializable;

public class AssociatonRule extends Tuple4<String, String, Double, Integer> implements Serializable {

    public AssociatonRule() {
    }

    public AssociatonRule(String key, String parent, Double confidence, Integer level) {
        super(key, parent, confidence, level);
    }

    public String getAssocItem() {
        return this.f0;
    }

    public String getParent() {
        return this.f1;
    }

    public Double getConfidence() {
        return this.f2;
    }

    public Integer getLevel() {
        return this.f3;
    }

}