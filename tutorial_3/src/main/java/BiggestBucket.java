import org.apache.flink.api.java.tuple.Tuple1;

public class BiggestBucket extends Tuple1<Integer> {

    public BiggestBucket() {
    }

    public BiggestBucket(Integer frequency) {
        super(frequency);
    }

    public Integer getFrequency() {
        return this.f0;
    }
}