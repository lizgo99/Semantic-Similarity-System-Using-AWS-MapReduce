import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.net.URI;
import java.util.*;

public class Step4 {

    public static class CompositeKey implements WritableComparable<CompositeKey> {
        private String originalKey;
        private String feature;
        private String isRelated;

        public CompositeKey() {
        }

        public CompositeKey(String originalKey, String feature, String isRelated) {
            this.originalKey = originalKey;
            this.feature = feature;
            this.isRelated = isRelated;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(originalKey);
            out.writeUTF(feature);
            out.writeUTF(isRelated);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            originalKey = in.readUTF();
            feature = in.readUTF();
            isRelated = in.readUTF();
        }

        @Override
        public int compareTo(CompositeKey other) {
            int cmp = this.originalKey.compareTo(other.originalKey);
            if (cmp != 0) {
                return cmp;
            }
            return this.feature.compareTo(other.feature);
        }

        public String getOriginalKey() {
            return originalKey;
        }

        public String getFeature() {
            return feature;
        }

        public String getIsRelated() {
            return isRelated;
        }
    }

    ///
    /// input: ?
    /// output: ?
    ///
    public static class MapperClass extends Mapper<LongWritable, Text, CompositeKey, Text> {
        public LinkedHashMap<String, HashSet<String>> GoldenStandard = new LinkedHashMap<>();
        private MultipleOutputs<CompositeKey, Text> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<>(context);

            Configuration conf = context.getConfiguration();
            String s3InputPath = conf.get("goldStandardPath");
            
            // String s3InputPath = "s3a://" + jarBucketName + "/test_gold_standard.txt";

            // Configure the FileSystem
            FileSystem fs = FileSystem.get(URI.create(s3InputPath), new Configuration());

            // Read the file
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(s3InputPath))))) {
                String line;

                while ((line = reader.readLine()) != null) {
                    // Split the line into parts
                    String[] parts = line.split("\\s+");

                    // Skip malformed lines
                    if (parts.length != 3) {
                        continue;
                    }

                    String word1 = parts[0];
                    String word2 = parts[1];
                    String isRelated = parts[2];

                    // Stemming
                    String word1Stemmed = Stemmer.stemWord(word1);
                    String word2Stemmed = Stemmer.stemWord(word2);

                    // Add words to the GoldenStandard map
                    addToGoldenStandard(word1Stemmed, word2Stemmed + " 1 " + isRelated);
                    addToGoldenStandard(word2Stemmed, word1Stemmed + " 0 " + isRelated);
                }
            }
        }

        /**
         * Helper method to add word relationships to the GoldenStandard map.
        */
        private void addToGoldenStandard(String key, String value) {
            GoldenStandard.computeIfAbsent(key, k -> new HashSet<>()).add(value);
        }



        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {

            String[] fields = line.toString().split("\\s+");
            if (fields.length != 6){
                return;
            }
            String lex = fields[0];
            String feat = fields[1];

            String assoc_freq = fields[2];
            String assoc_prob = fields[3];
            String assoc_PMI = fields[4];
            String assoc_t_test = fields[5];

             if (GoldenStandard.containsKey(lex)){
                 String w1;
                 String w2;
                for (String val : GoldenStandard.get(lex)){
                    String[] wordToPos = val.split(" ");
                    String pos = wordToPos[1];
                    String isRelated = wordToPos[2];

                    if (pos.equals("0")) {
                        w1 = wordToPos[0];
                        w2 = lex;
                    } else {
                        w1 = lex;
                        w2 = wordToPos[0];
                    }
//                    multipleOutputs.write("debugOutput", new Text(String.format("[DEBUG] mapper key: %s %s", w1,w2)), new Text(String.format("pos:%s isRelated:%s", pos, isRelated)));

                    String key = String.format("%s %s", w1, w2);
                    CompositeKey compositeKey = new CompositeKey(key, feat, isRelated);
                    context.write(compositeKey,
                            new Text(String.format("%s %s %s %s %s %s", feat, lex, assoc_freq, assoc_prob, assoc_PMI, assoc_t_test)));
                }
             }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }


    public static class ReducerClass extends Reducer<CompositeKey, Text, Text, Text> {
        private MultipleOutputs<Text, Text> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        public final String[] ZEROS = {"_","_","0=0","0=0","0=0","0=0"};

        public double[] distManhattan = new double[4];
        public double[] distEuclidean = new double[4];
        public double[][] simCosine = new double[4][3];
        public double[][] simJaccard = new double[4][2];
        public double[][] simDice = new double[4][2];
        public double[][] simJS = new double[4][2];


        /** DIFF-MATRIX:
         *              distManhattan   distEuclidean   simCosine   simJaccard  simDice  simJS
         * assoc_freq
         * assoc_prob
         * assoc_PMI
         * assoc_t_test
         */

        @Override
        public void reduce(CompositeKey compKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Split the key into words
            String key = compKey.getOriginalKey();
            String isRelated = compKey.getIsRelated();
            String[] words = key.split("\\s+");
            if (words.length != 2) {
                return; // Invalid key format, skip processing
            }

            String w1 = words[0];
            String w2 = words[1];

            String[] lastVal = null;

            for (Text val : values) {
//                multipleOutputs.write("debugOutput", new Text(String.format("[DEBUG] reducer key: %s %s %s", w1,w2, isRelated)), new Text("val: " + val.toString()));
                String[] parts = val.toString().split("\\s+");
                if (parts.length != 6) {
                    continue;
                }
                // Handle the first value
                if (lastVal == null) {
                    lastVal = parts;
                    continue;
                }

                // Compare lastVal and parts
                if (lastVal[0].equals(parts[0])) { // Complete pair
                    if (lastVal[1].equals(w1)) {
                        calculateDiff(lastVal, parts);
                    } else {
                        calculateDiff(parts, lastVal);
                    }
                    lastVal = null;
                } else { // Incomplete pair
                    if (lastVal[1].equals(w1)) {
                        calculateDiff(lastVal, ZEROS);
                    } else {
                        calculateDiff(ZEROS, lastVal);
                    }
                    lastVal = parts;
                }
            }

            // Handle the last value if needed (in case of an incomplete pair)
            if (lastVal != null) {
                if (lastVal[1].equals(w1)) {
                    calculateDiff(lastVal, ZEROS);
                } else {
                    calculateDiff(ZEROS, lastVal);
                }
            }

            // Final calculations
            double[][] diffMetrix = new double[4][6];

            // CHECK DIVISION BY ZERO ?

            for (int i = 0; i < 4; i++) {
                diffMetrix[i][0] = distManhattan[i];
                diffMetrix[i][1] = Math.sqrt(distEuclidean[i]);
                diffMetrix[i][2] = simCosine[i][1] != 0 && simCosine[i][2] != 0 ?
                        simCosine[i][0] / (Math.sqrt(simCosine[i][1]) * Math.sqrt(simCosine[i][2])) : Double.NaN;
                diffMetrix[i][3] = simJaccard[i][1] != 0 ?
                        simJaccard[i][0] / simJaccard[i][1] : Double.NaN;
                diffMetrix[i][4] = simDice[i][1] != 0 ?
                        2 * simDice[i][0] / simDice[i][1] : Double.NaN;
                diffMetrix[i][5] = simJS[i][0] + simJS[i][1];
            }

            double[] flattenDiffMatrix = Arrays.stream(diffMetrix)
                                                .flatMapToDouble(Arrays::stream)
                                                .toArray();

            // context.write(new Text(String.format("%s %s %s", w1, w2, isRelated)), new Text(Arrays.deepToString(diffMetrix)));
            context.write(new Text(String.format("%s %s %s", w2, w1, isRelated)), new Text(Arrays.toString(flattenDiffMatrix)));

            cleanDiffMatrix();

        }

        private void calculateDiff(String[] l1,String[] l2) throws IOException, InterruptedException {
            if (l1.length != l2.length){
                return;
            }

            for (int i = 2; i < l1.length; i++) {

                double val1 = Double.parseDouble(l1[i].split("=")[1]);
                double val2 = Double.parseDouble(l2[i].split("=")[1]);
                
                handleDistManhattan(i-2, val1, val2);
                handleDistEuclidean(i-2, val1, val2);
                try {
                    handleSimCosine(i-2, val1, val2);
                } catch (IOException | InterruptedException e) {
                    continue;
//                    multipleOutputs.write("debugOutput", new Text("ERROR"), new Text(String.format("SimCosine val1:%s val2:%s", val1,val2)));
                }
                handleSimJaccard(i-2, val1, val2);
                handleSimDice(i-2, val1, val2);
                handleSimJS(i-2, val1, val2);
            }
        }

        private void handleDistManhattan(int i, double val1, double val2){
            distManhattan[i] += Math.abs(val1 - val2);
        }

        private void handleDistEuclidean(int i, double val1, double val2){
            distEuclidean[i] += (val1 + val2) * (val1 + val2);
        }

        private void handleSimCosine(int i, double val1, double val2) throws IOException, InterruptedException {
            simCosine[i][0] += (val1 * val2);
            simCosine[i][1] += val1 * val1;
            simCosine[i][2] += val2 * val2;
        }

        private void handleSimJaccard(int i, double val1, double val2){

            simJaccard[i][0] += Math.min(val1,val2);
            simJaccard[i][1] += Math.max(val1,val2);
        }

        private void handleSimDice(int i, double val1, double val2){

            simDice[i][0] += Math.min(val1,val2);
            simDice[i][1] += val1 + val2;
        }

        private void handleSimJS(int i, double val1, double val2) {
            double mean = (val1 + val2) / 2.0;

            // When both values are 0, the contribution to JS divergence is 0
            if (val1 == 0 && val2 == 0) {
                simJS[i][0] = 0;
                simJS[i][1] = 0;
                return;
            }
            // When val1 is 0, its contribution to KL divergence is 0
            simJS[i][0] += (val1 > 0) ? val1 * Math.log(val1 / mean) : 0;
            // When val2 is 0, its contribution to KL divergence is 0
            simJS[i][1] += (val2 > 0) ? val2 * Math.log(val2 / mean) : 0;
        }
        
        private void cleanDiffMatrix() {
            Arrays.fill(distManhattan, 0);
            Arrays.fill(distEuclidean, 0);

            for (int i = 0; i < 4; i++) {
                Arrays.fill(simCosine[i], 0);
                Arrays.fill(simDice[i], 0);
                Arrays.fill(simJaccard[i], 0);
                Arrays.fill(simJS[i], 0);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    public static class PartitionerClass extends Partitioner<CompositeKey, Text> {
        @Override
        public int getPartition(CompositeKey key, Text value, int numPartitions) {
            return (key.getOriginalKey().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class CompositeKeyComparator extends WritableComparator {
        protected CompositeKeyComparator() {
            super(CompositeKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            CompositeKey key1 = (CompositeKey) a;
            CompositeKey key2 = (CompositeKey) b;
            return key1.compareTo(key2);
        }
    }

    public static class OriginalKeyGroupingComparator extends WritableComparator {
        protected OriginalKeyGroupingComparator() {
            super(CompositeKey.class, true);
        }
    
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            CompositeKey key1 = (CompositeKey) a;
            CompositeKey key2 = (CompositeKey) b;
            return key1.getOriginalKey().compareTo(key2.getOriginalKey());
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 4 started!");

        // String jarBucketName = "classifierinfo1";

        String jarBucketName = args[1];
        String inputPath = args[2];
        String outputPath = args[3];
        String goldStandardPath = args[4];

        Configuration conf = new Configuration();
        conf.set("goldStandardPath", goldStandardPath);

        Job job = Job.getInstance(conf, "Step4");

        job.setJarByClass(Step4.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
//        job.setCombinerClass(ReducerClass.class);

        job.setGroupingComparatorClass(OriginalKeyGroupingComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);
        job.setPartitionerClass(Step4.PartitionerClass.class);

        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath + "part-r*"));

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

//        MultipleOutputs.addNamedOutput(job, "debugOutput", TextOutputFormat.class, Text.class, Text.class);

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
