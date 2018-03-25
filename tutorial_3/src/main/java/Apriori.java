import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class Apriori {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);


        //local example execution parameters: ./bin/flink run ~/IdeaProjects/apriori/target/reintippan-1.0-SNAPSHOT.jar --datapath /home/phil/IdeaProjects/apriori/src/main/java/data/BMS-POS.dat --output /home/phil/IdeaProjects/apriori/output.txt --parallelism 1 --iterations 3
        // ./../flink-1.3.2/bin/flink run ~/git-projects/bigData_2017_18/tutorial_3/target/tutorial_3-1.0-SNAPSHOT.jar --datapath ~/git-projects/bigData_2017_18/tutorial_3/data/BMS-POS.dat --output ~/git-projects/bigData_2017_18/tutorial_3/data/output/output --parallelism 1 --iterations 3
        //DEFINE PARAMETER VALUES
        int parall;
        double minSup;
        int maxIterations;
        String inputpath = null;
        String outputPath = null;

        if (params.has("datapath") && params.has("output")) {
            try {
                inputpath = params.get("datapath");
                outputPath = params.get("output");
            } catch (Exception e) {
                System.out.println("Please specify parameters datapath and output");
            }
        }

        //SET PARALLELISM
        parall = params.getInt("parallelism", 2);
        env.setParallelism(parall);

        //GET DATA
        DataSet<Datapoint> data = getData(env, inputpath);

        //GET RECORDS
        DataSet<Bucket> records = data.groupBy(0).reduceGroup(new ReduceToBuckets());

        //GET AMOUNT OF MAX. ITERATIONS
        BiggestBucket biggestBucket = records.map(new MapFunction<Bucket, BiggestBucket>() {
            public BiggestBucket map(Bucket datapoint) throws Exception {
                return new BiggestBucket(datapoint.getItems().size());
            }
        }).maxBy(0).collect().get(0);
        maxIterations = biggestBucket.getSize();

        //GET AMOUNT OF BUCKETS
        long amountRecords = records.map(new MapFunction<Bucket, Integer>() {
            public Integer map(Bucket itemset) throws Exception {
                return itemset.getBid();
            }
        }).count();

        //SET ALGORITHM PARAMETERS
        minSup = params.getDouble("threshold", 0.05);
        maxIterations = params.getInt("iterations", maxIterations);

        //GET RELATIVE MIN SUPPORT / THRESHOLD
        final double thresh = amountRecords * minSup;

        //GET l1
        DataSet<Candidate> l1 = generateCandidatesL1(data, thresh);

        //Iterate:
        DeltaIteration<Candidate, Candidate> iteration = l1.iterateDelta(l1, maxIterations, 0).parallelism(1);

        DataSet<Candidate> ck = generateCandidates(iteration.getWorkset());
        //DataSet<Candidate> pCk = pruneToLargeItemsets(ck);
        DataSet<Candidate> lk = filterLargeItemsets(ck, records, thresh);

        DataSet<Candidate> result = iteration.closeWith(iteration.getWorkset(), lk);

        //Sort and print results
        result.sortPartition(4, Order.ASCENDING).setParallelism(1).writeAsFormattedText(outputPath + ".txt", FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<Candidate>() {
            @Override
            public String format(Candidate candidate) {
                return String.format("%s from  k-1 list %s with frequence %s", candidate.getItems().toString(), candidate.getParent(), candidate.getFrequency());
            }
        }).setParallelism(1);

        generateAssociationRules(result, amountRecords).sortPartition(3, Order.ASCENDING).setParallelism(1).writeAsFormattedText(outputPath + "_assoc.txt", FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<AssociatonRule>() {
            @Override
            public String format(AssociatonRule assoc) {
                return String.format("%s -> %s with confidence %s", assoc.getAssocItem(), assoc.getParent(), assoc.getConfidence());
            }
        }).setParallelism(1);

        env.execute("Apriori-Algo");

    }

    private static DataSet<Datapoint> getData(ExecutionEnvironment env, String path) {
        return env.readCsvFile(path)
                .includeFields("11")
                .fieldDelimiter("\t")
                .lineDelimiter("\n")
                .tupleType(Datapoint.class);
    }

    private static DataSet<Candidate> generateCandidatesL1(DataSet<Datapoint> data, Double thresh) {
        DataSet<Candidate> l1 = data.map(new MapFunction<Datapoint, Candidate>() {
            public Candidate map(Datapoint datapoint) throws Exception {
                return new Candidate(Arrays.asList(datapoint.getItem()).toString(), "$", new ArrayList<>(Arrays.asList(datapoint.getItem())), 1, 1);
            }
        })
                .groupBy(0)
                .reduce(new ReduceFunction<Candidate>() {
                    public Candidate reduce(Candidate b1, Candidate b2) throws Exception {
                        return new Candidate(b1.getKey(), b1.getParent(), b1.getItems(), b1.getFrequency() + b2.getFrequency(), b1.getItems().size());
                    }
                })
                .filter(new FilterFunction<Candidate>() {
                    public boolean filter(Candidate itemset) throws Exception {
                        return itemset.getFrequency() >= thresh;
                    }
                });
        return l1;
    }

    private static DataSet<Candidate> generateCandidates(DataSet<Candidate> dataSet) {
        DataSet<Candidate> ck = dataSet.join(dataSet).where(1).equalTo(1).with(new JoinFunction<Candidate, Candidate, Candidate>() {
            public Candidate join(Candidate candidate1, Candidate candidate2) throws Exception {
                //sorting
                ArrayList<Integer> newCandidate = candidate1.getItems();
                Collections.sort(newCandidate);
                newCandidate.add(candidate2.getItems().get(candidate2.getItems().size() - 1));
                return new Candidate(newCandidate.toString(), newCandidate.subList(0, newCandidate.size() - 1).toString(), newCandidate, 1, newCandidate.size());
            }
        }).filter(new FilterFunction<Candidate>() {
            public boolean filter(Candidate candidate) throws Exception {
                Integer lastBucketItem = candidate.getItems().get(candidate.getItems().size() - 1);
                Integer secondLastBucketItem = candidate.getItems().get(candidate.getItems().size() - 2);
                return secondLastBucketItem < lastBucketItem;
            }
        });
        return ck;
    }

    private static DataSet<Candidate> pruneToLargeItemsets(DataSet<Candidate> dataSet) {
        DataSet<String> parents = dataSet.map(new MapFunction<Candidate, String>() {
            public String map(Candidate candidate) throws Exception {
                return candidate.getParent();
            }
        }).distinct();

        DataSet<Candidate> pCk = dataSet
                .filter(new FilterPruningFunction())
                .withBroadcastSet(parents, "parents");
        return pCk;
    }

    private static DataSet<Candidate> filterLargeItemsets(DataSet<Candidate> ck, DataSet<Bucket> records, Double thresh) {
        DataSet<Candidate> lk = records.flatMap(new ComputeFrequenciesBuckets())
                .withBroadcastSet(ck, "candidates")
                .groupBy(0)
                .reduce(new ReduceFunction<Candidate>() {
                    public Candidate reduce(Candidate b1, Candidate b2) throws Exception {
                        return new Candidate(b1.getKey(), b1.getParent(), b1.getItems(), b1.getFrequency() + b2.getFrequency(), b1.getItems().size());
                    }
                })
                .filter(new FilterFunction<Candidate>() {
                    public boolean filter(Candidate itemset) throws Exception {
                        return itemset.getFrequency() >= thresh;
                    }
                });
        return lk;
    }

    private static DataSet<AssociatonRule> generateAssociationRules(DataSet<Candidate> result, long amountRecords) {
        DataSet<AssociatonRule> assocs = result.join(result).where(0).equalTo(1).with(new JoinFunction<Candidate, Candidate, AssociatonRule>() {
            public AssociatonRule join(Candidate candidate1, Candidate candidate2) throws Exception {
                Double suppCand1 = candidate1.getFrequency().doubleValue() / amountRecords;
                Double suppCand2 = candidate2.getFrequency().doubleValue() / amountRecords;
                Double confidence = suppCand2 / suppCand1;
                return new AssociatonRule(candidate2.getParent(), candidate2.getItems().get(candidate2.getItems().size() - 1).toString(), confidence, candidate2.getItems().size());
            }
        });
        return assocs;
    }
}

class ReduceToBuckets implements GroupReduceFunction<Datapoint, Bucket> {
    public void reduce(Iterable<Datapoint> data, Collector<Bucket> bucket) {
        ArrayList<Integer> items = new ArrayList<>();
        Integer bid = null;

        for (Datapoint d : data) {
            items.add(d.getItem());
            bid = d.getBid();
        }

        bucket.collect(new Bucket(bid, items));
    }
}

class FilterPruningFunction extends RichFilterFunction<Candidate> {
    private Collection<String> parents;

    @Override
    public void open(Configuration parameters) {
        this.parents = getRuntimeContext().getBroadcastVariable("parents");
    }

    @Override
    public boolean filter(Candidate candidate) throws Exception {
        ArrayList<String> itemsets = new ArrayList<>();

        ArrayList<Integer> items = candidate.getItems();


        for (Integer item : items) {
            ArrayList<Integer> itemset = candidate.getItems();
            itemset.remove(item);
            itemsets.add(itemset.toString());
        }

        return parents.containsAll(itemsets);
    }
}

class ComputeFrequencies extends RichMapFunction<Candidate, Candidate> {
    private Collection<Bucket> records;

    @Override
    public void open(Configuration parameters) {
        this.records = getRuntimeContext().getBroadcastVariable("records");
    }

    @Override
    public Candidate map(Candidate candidate) {
        int frequency = 0;

        for (Bucket record : this.records) {
            if (record.getItems().containsAll(candidate.getItems())) {
                frequency++;
            }
        }
        return new Candidate(candidate.getKey(), candidate.getParent(), candidate.getItems(), frequency, candidate.getLevel());
    }
}

class ComputeFrequenciesBuckets extends RichFlatMapFunction<Bucket, Candidate> {
    private Collection<Candidate> candidates;

    @Override
    public void open(Configuration parameters) {
        this.candidates = getRuntimeContext().getBroadcastVariable("candidates");
    }

    @Override
    public void flatMap(Bucket record, Collector<Candidate> collector) throws Exception {
        for (Candidate candidate : this.candidates) {
            if (record.getItems().containsAll(candidate.getItems())) {
                collector.collect(new Candidate(candidate.getKey(), candidate.getParent(), candidate.getItems(), 1, candidate.getLevel()));
            }
        }
    }
}
