import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

import static java.lang.Math.toIntExact;


@SuppressWarnings("serial")
public class Apriori {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        Tuple3<Integer, Double, String> initializedParams = readParams(params);

        int parallels = initializedParams.f0;
        double minSup = initializedParams.f1;
        String filename = initializedParams.f2;

        DataSet<Tuple2<Integer, Integer>> dataSet = readData(env, filename);

        dataSet.groupBy(0).reduce( (a, b) -> new Tuple2<>(a.f0 + b.f0);
    }

    public static Tuple3<Integer, Double, String> readParams(ParameterTool params) {
        final String PARALLELIZATION = "parallelization";
        final String FILENAME = "filename";
        final String MIN_SUP = "minsup";

        int parallels = 0;
        double minSup = 0.0;
        String filename = null;

        try {
            parallels = params.getInt(PARALLELIZATION, 1);
        } catch (NumberFormatException e) {
            System.err.printf("Could not read parameter %s.", PARALLELIZATION);
            e.printStackTrace();
        }
        try {
            minSup = params.getDouble(MIN_SUP, 0.5);
        } catch (Exception e) {
            System.err.printf("Could not read parameter %s.", MIN_SUP);
            e.printStackTrace();
        }
        try {
            filename = params.get(FILENAME, "BMS-POS.dat");
        } catch (Exception e) {
            System.err.printf("Could not read parameter %s.", FILENAME);
            e.printStackTrace();
        }

        return new Tuple3(parallels, minSup, filename);
    }

    public static DataSet<Tuple2<Integer, Integer>> readData(ExecutionEnvironment env, String filename) {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        String path = classloader.getResource(filename).getPath();
        System.out.println(path);

        return env.readCsvFile(path)
                .includeFields("11")
                .fieldDelimiter("\t")
                .lineDelimiter("\n")
                .types(Integer.class, Integer.class);
    }
}