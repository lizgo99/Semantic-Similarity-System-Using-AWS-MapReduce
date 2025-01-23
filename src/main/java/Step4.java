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

        public CompositeKey() {
        }

        public CompositeKey(String originalKey, String feature) {
            this.originalKey = originalKey;
            this.feature = feature;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(originalKey);
            out.writeUTF(feature);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            originalKey = in.readUTF();
            feature = in.readUTF();
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
    }

    ///
    /// input: ?
    /// output: ?
    ///
    public static class MapperClass extends Mapper<LongWritable, Text, CompositeKey, Text> {
        public LinkedHashMap<String, HashSet<String>> GoldenStandard = new LinkedHashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String jarBucketName = "classifierinfo1";
//            String s3InputPath = "s3a://" + jarBucketName + "/word-relatedness.txt";
            String s3InputPath = "s3a://" + jarBucketName + "/test_gold_standard.txt";

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
//                    String isRelated = parts[2];

                    // Add words to the GoldenStandard map
                    addToGoldenStandard(word1, word2 + " 1");
                    addToGoldenStandard(word2, word1 + " 0");
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
                    if (wordToPos[1].equals("0")) {
                        w1 = wordToPos[0];
                        w2 = lex;
                    } else {
                        w1 = lex;
                        w2 = wordToPos[0];
                    }
                    String key = String.format("%s %s", w1, w2);
                    CompositeKey compositeKey = new CompositeKey(key, feat);
                    context.write(compositeKey,
                            new Text(String.format("%s %s %s %s %s %s", feat, lex, assoc_freq, assoc_prob, assoc_PMI, assoc_t_test)));

                }
            
             }

                // about us-pobj   _ _ _ _
                // in Golden
                // about in
                // about else
                // what about
                // about {0 in True}
                // in {1 about True}
        }
    }


    public static class ReducerClass extends Reducer<CompositeKey, Text, Text, Text> {
        public final String[] ZEROS = {"_","_","0=0","0=0","0=0","0=0"};

        public double[] distManhattan = new double[4];
        public double[] distEuclidean = new double[4];
        public double[][] simCosine = new double[4][3];
        public double[][] simJaccard = new double[4][2];
        public double[][] simDice = new double[4][2];
        public double[][] simJS = new double[4][2];

        private MultipleOutputs<Text, Text> multipleOutputs;

        /**
         *              distManhattan   distEuclidean   simCosine   simJaccard  simDice  simJS
         * assoc_freq
         * assoc_prob
         * assoc_PMI
         * assoc_t_test
         */

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        public void reduce(CompositeKey compKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Split the key into words
            String key = compKey.getOriginalKey();
            String[] words = key.toString().split("\\s+");
            if (words.length != 2) {
                return; // Invalid key format, skip processing
            }

            String w1 = words[0];
            String w2 = words[1];

            String[] lastVal = null;



            for (Text val : values) {
                String[] parts = val.toString().split("\\s+");
                if (parts.length != 6) {
                    continue;
                }
                multipleOutputs.write("keyValueOutput", key, val);
                context.write(new Text(String.format("[DEBUG] key: %s %s | val:", w1, w2)) , val);
                // Handle the first value
                if (lastVal == null) {
                    lastVal = parts;
                    continue;
                }

                // Compare lastVal and parts
                if (lastVal[0].equals(parts[0])) { // Complete pair
                    if (lastVal[1].equals(w1)) {
                        context.write(new Text(String.format("[DEBUG1] lastVal: %s", Arrays.toString(lastVal))), new Text(String.format("parts: %s", Arrays.toString(parts))));
                        calculateDiff(lastVal, parts);
                    } else {
                        context.write(new Text(String.format("[DEBUG2] lastVal: %s", Arrays.toString(lastVal))), new Text(String.format("parts: %s", Arrays.toString(parts))));
                        calculateDiff(parts, lastVal);
                    }
                    lastVal = null;
                } else { // Incomplete pair
                    if (lastVal[1].equals(w1)) {
                        context.write(new Text(String.format("[DEBUG3] lastVal: %s", Arrays.toString(lastVal))), new Text(String.format("parts: %s", Arrays.toString(ZEROS))));
                        calculateDiff(lastVal, ZEROS);
                    } else {
                        context.write(new Text(String.format("[DEBUG4] lastVal: %s", Arrays.toString(lastVal))), new Text(String.format("parts: %s", Arrays.toString(ZEROS))));
                        calculateDiff(ZEROS, lastVal);
                    }
                    lastVal = parts;
                }
            }

            // Handle the last value if needed (in case of an incomplete pair)
            if (lastVal != null) {
                if (lastVal[1].equals(w1)) {
                    context.write(new Text(String.format("[DEBUG5] lastVal: %s", Arrays.toString(lastVal))), new Text(String.format("parts: %s", Arrays.toString(ZEROS))));
                    calculateDiff(lastVal, ZEROS);
                } else {
                    context.write(new Text(String.format("[DEBUG6] lastVal: %s", Arrays.toString(lastVal))), new Text(String.format("parts: %s", Arrays.toString(ZEROS))));
                    calculateDiff(ZEROS, lastVal);
                }
            }

            // Final calculations
            double[][] diffMetrix = new double[4][6];

            for (int i = 0; i < 4; i++) {
                diffMetrix[i][0] = distManhattan[i];
                diffMetrix[i][1] = Math.sqrt(distEuclidean[i]);
                diffMetrix[i][2] = simCosine[i][0] / (Math.sqrt(simCosine[i][1]) * Math.sqrt(simCosine[i][2]));
                diffMetrix[i][3] = simJaccard[i][0] / simJaccard[i][1];
                diffMetrix[i][4] = 2 * simDice[i][0] / simDice[i][1];
                diffMetrix[i][5] = simJS[i][0] + simJS[i][1];
            }

            context.write(new Text(String.format("%s %s", w1,w2)), new Text(Arrays.deepToString(diffMetrix)));
            clean();
        }

        private void calculateDiff(String[] l1,String[] l2){
            if (l1.length != l2.length){
                return;
            }

            for (int i = 2; i < l1.length; i++) {

                double val1 = Double.parseDouble(l1[i].split("=")[1]);
                double val2 = Double.parseDouble(l2[i].split("=")[1]);

                handleDistManhattan(i-2, val1, val2);
                handleDistEuclidean(i-2, val1, val2);
                handleSimCosine(i-2, val1, val2);
                handleSimJaccard(i-2, val1, val2);
                handleSimDice(i-2, val1, val2);
                handleSimJS(i-2, val1, val2);
            }
        }

        private void handleDistManhattan(int i, double val1, double val2){
            distManhattan[i] += Math.abs(val1 - val2);
        }

        private void handleDistEuclidean(int i, double val1, double val2){
            distEuclidean[i] += Math.pow((val1 + val2), 2);
        }

        private void handleSimCosine(int i, double val1, double val2){
            simCosine[i][0] += (val1 * val2);
            simCosine[i][1] += Math.pow(val1, 2);
            simCosine[i][2] += Math.pow(val2, 2);
        }

        private void handleSimJaccard(int i, double val1, double val2){

            simJaccard[i][0] += Math.min(val1,val2);
            simJaccard[i][1] += Math.max(val1,val2);
        }

        private void handleSimDice(int i, double val1, double val2){

            simDice[i][0] += Math.min(val1,val2);
            simDice[i][1] += val1 + val2;
        }

        private void handleSimJS(int i, double val1, double val2){

            double mean = (val1 + val2) / 2.0;
            if (val1 > 0 || val2 > 0){
                simJS[i][0] += val1 * Math.log(val1 / mean);
                simJS[i][1] += val2 * Math.log(val2 / mean);
            }
        }
        
        private void clean() {
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

        String jarBucketName = "classifierinfo1";

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Step4");

//        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", 64 * 1024 * 1024); // 64MB (default is 128MB)

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

        MultipleOutputs.addNamedOutput(job, "keyValueOutput", TextOutputFormat.class, Text.class, Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("s3://" + jarBucketName + "/step3_output/"));

        FileOutputFormat.setOutputPath(job, new Path("s3://" + jarBucketName + "/step4_output/"));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
