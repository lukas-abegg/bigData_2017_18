import org.apache.flink.api.java.tuple.Tuple1;

public class BiggestBucket extends Tuple1<Integer> {

    public BiggestBucket() {
    }

    public BiggestBucket(Integer size) {
        super(size);
    }

    public Integer getSize() {
        return this.f0;
    }
}