import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;

public class Apriori {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);


        //GET PARAMETER VALUES
        int parall = 1;
        double minSup = 0.1;

        String path = "/Users/lukas/git-projects/bigData_2017_18/tutorial_3/data/BMS-POS.dat";

        if(params.has("parallel") && params.has("threshold") && params.has("path")){
            try {
                parall = params.getInt("parallel");
                minSup = params.getDouble("threshold");
                path = params.get("path");
            }catch (Exception e){
                System.out.println("Wrong Parameters!");
            }
        }

        //set parallelism
        env.setParallelism(parall);

        List<Tuple2<ArrayList<Integer>, Integer>> dataSets = new ArrayList<>();


        //GET DATA
        DataSet<Tuple2<Integer, Integer>> data = getData(env, path);

        //GET RECORDS
        DataSet<Tuple2<Integer, ArrayList<Integer>>> records = data.groupBy(0).reduceGroup(new reduceToBuckets());

        //GET AMOUNT OF MAX. ITERATIONS
        Tuple1<Integer> biggestBucket = records.map(new MapFunction<Tuple2<Integer,ArrayList<Integer>>, Tuple1<Integer>>(){
            public Tuple1<Integer> map(Tuple2<Integer, ArrayList<Integer>> datapoint) throws Exception{
                //Tuple1<Integer> size = new Tuple1<Integer>();
                //size.setField(datapoint.f1.size(),0);
                return new Tuple1<Integer>(datapoint.f1.size());
            }
        }).maxBy(0).collect().get(0);

        Integer maxIterations = biggestBucket.f0;

        //GET AMOUNT OF BUCKETS
        long amountRecords = records.map(new MapFunction<Tuple2<Integer,ArrayList<Integer>>, Integer>() {
            public Integer map(Tuple2<Integer, ArrayList<Integer>> itemset) throws Exception {
                return itemset.f0;
            }
        }).count();

        //GET ACTUAL MIN SUPPORT / THRESHOLD
        final double thresh = 40000;//amountRecords*minSup;

        //GET L_1
        DataSet<Tuple2<ArrayList<Integer>, Integer>> L1 = data.map(new MapFunction<Tuple2<Integer,Integer>, Tuple2<Integer, Integer>>() {
            public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> datapoint) throws Exception {
                return new Tuple2<Integer, Integer>(datapoint.f1, 1);
            }
        })
                .groupBy(0)
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> b1, Tuple2<Integer, Integer> b2) throws Exception {
                        return new Tuple2<Integer, Integer>(b1.f0, b1.f1+b2.f1);
                    }
                })
                .filter(new FilterFunction<Tuple2<Integer, Integer>>() {
                    public boolean filter(Tuple2<Integer, Integer> itemset) throws Exception {
                        return itemset.f1 >= thresh;
                    }
                }) //828
                .map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<ArrayList<Integer>, Integer>>() {
                    public Tuple2<ArrayList<Integer>, Integer> map(Tuple2<Integer, Integer> itemset) throws Exception {
                        return new Tuple2<ArrayList<Integer>, Integer>(new ArrayList<Integer>(Arrays.asList(itemset.f0)), itemset.f1);
                    }
                });


        //START ITERATION HERE
        //Iterate:
        IterativeDataSet<Tuple2<ArrayList<Integer>, Integer>> init = L1.iterate(maxIterations - 1);

        DataSet<Tuple1<ArrayList<Integer>>> Ck = init.cross(L1).with(new CrossFunction<Tuple2<ArrayList<Integer>, Integer>, Tuple2<ArrayList<Integer>, Integer>, Tuple1<ArrayList<Integer>>>() {
            public Tuple1<ArrayList<Integer>> cross(Tuple2<ArrayList<Integer>, Integer> subset1, Tuple2<ArrayList<Integer>, Integer> subset2) throws Exception {
                ArrayList<Integer> items = new ArrayList<Integer>();
                items.addAll(subset1.f0);

                if(subset1.f0.size() > 1) {
                    if (subset1.f0.subList(0, subset1.f0.size() - 2).equals(subset2.f0.subList(0, subset1.f0.size() - 2))
                            && subset1.f0.get(subset1.f0.size() - 1) < subset2.f0.get(subset2.f0.size() - 1)) {
                        items.add(subset2.f0.get(subset2.f0.size() - 1));
                    } else {
                        return new Tuple1<ArrayList<Integer>>(new ArrayList<Integer>(Arrays.asList(-1)));
                    }
                } else {
                    if(subset1.f0.get(subset1.f0.size() - 1) < subset2.f0.get(subset2.f0.size() - 1)) {
                        items.add(subset2.f0.get(subset2.f0.size() - 1));
                    } else {
                        return new Tuple1<ArrayList<Integer>>(new ArrayList<Integer>(Arrays.asList(-1)));
                    }
                }
                Collections.sort(items);
                return new Tuple1<ArrayList<Integer>>(items);
            }
        });

        DataSet<Tuple2<ArrayList<Integer>, Integer>> Lk = Ck.map(new ComputeFrequencies()).withBroadcastSet(records, "records")
                .filter(new FilterFunction<Tuple2<ArrayList<Integer>, Integer>>() {
                    public boolean filter(Tuple2<ArrayList<Integer>, Integer> itemset) throws Exception {
                       return itemset.f1 >= thresh;
                    }
                });

        DataSet<Tuple2<ArrayList<Integer>, Integer>> output = init.closeWith(Lk, Lk);


        //env.fromCollection(dataSets).writeAsText("/Users/lukas/git-projects/bigData_2017_18/tutorial_3/output.txt");
        output.writeAsText("/Users/lukas/git-projects/bigData_2017_18/tutorial_3/output.txt");

        env.execute("Apriori-Algo");

    }

    public static DataSet<Tuple2<Integer, Integer>> getData(ExecutionEnvironment env, String path){
        return env.readCsvFile(path)
                .includeFields("11")
                .fieldDelimiter("\t")
                .lineDelimiter("\n")
                .types(Integer.class, Integer.class);
    }
}

class reduceToBuckets implements GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, ArrayList<Integer>>> {
    public void reduce(Iterable<Tuple2<Integer, Integer>> data, Collector<Tuple2<Integer, ArrayList<Integer>>> bucket) throws Exception {
        ArrayList<Integer> items = new ArrayList<Integer>();
        Integer bid = null;
        for (Tuple2<Integer, Integer> d : data){
            items.add(d.f1);
            bid = d.f0;
        }
        bucket.collect(new Tuple2<Integer, ArrayList<Integer>>(bid, items));
    }
}

class ComputeFrequencies extends RichMapFunction<Tuple1<ArrayList<Integer>>, Tuple2<ArrayList<Integer>, Integer>>{
    private Collection<Tuple2<Integer, ArrayList<Integer>>> records;

    @Override
    public void open(Configuration parameters) throws Exception{
        this.records = getRuntimeContext().getBroadcastVariable("records");
    }

    @Override
    public Tuple2<ArrayList<Integer>, Integer> map(Tuple1<ArrayList<Integer>> candidate){
        int frequency = 0;

        for(Tuple2<Integer, ArrayList<Integer>> record : this.records){
            if(record.f1.containsAll(candidate.f0)){
                frequency++;
            }
        }
        return new Tuple2<ArrayList<Integer>, Integer>(candidate.f0, frequency);
    }
}