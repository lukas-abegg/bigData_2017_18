import org.apache.flink.api.java.tuple.Tuple2;

public class Datapoint extends Tuple2<Integer, Integer> {

    public Datapoint() {
    }

    public Integer getBid() {
        return this.f0;
    }

    public Integer getItem() {
        return this.f1;
    }
}