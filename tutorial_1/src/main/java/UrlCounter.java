import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UrlCounter {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        
        final String INPUT = "input";
        final String OUTPUT = "output";
        final Pattern pattern = Pattern.compile("(\"\\w+\\s+)(.+?)(\\s+HTTP)");

        if(params.has(INPUT)) {
            DataSet<String> log = env.readTextFile(params.get(INPUT));
            
            DataSet<Tuple2<String, Integer>> urls = log.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

                public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
                    Matcher matcher = pattern.matcher(s);
                    if (matcher.find() && matcher.group(2).length() > 0) {
                        out.collect(Tuple2.of(matcher.group(2), 1));
                    }
                }
            }).groupBy(0).sum(1).sortPartition(1, Order.DESCENDING).setParallelism(1);

            if (params.has(OUTPUT)) {
                urls.writeAsText(params.get(OUTPUT));
                env.execute("Url Counter");
            }
        }
    }
}
