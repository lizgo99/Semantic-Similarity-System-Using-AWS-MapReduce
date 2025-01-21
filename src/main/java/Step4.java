import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;


import java.io.*;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.LinkedList;


public class Step4 {
    ///
    /// input: ?
    /// output: ?
    ///
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        public LinkedHashMap<String, LinkedList<String>> GoldenStandard = new LinkedHashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String jarBucketName = "classifierinfo";
            String s3InputPath = "s3a://" + jarBucketName + "/word-relatedness.txt";

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

                    // Add words to the GoldenStandard map
                    addToGoldenStandard(parts[0], parts[1]);
                    addToGoldenStandard(parts[1], parts[0]);
                }
            }
        }

        /**
         * Helper method to add word relationships to the GoldenStandard map.
         */
        private void addToGoldenStandard(String key, String value) {
            GoldenStandard.computeIfAbsent(key, k -> new LinkedList<>()).add(value);
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
                for (String val : GoldenStandard.get(lex)){
                    context.write(new Text(String.format("%s %s", lex, val)),
                            new Text(String.format("%s %s assoc_freq=%s assoc_prob=%s assoc_PMI=%s assoc_t_test=%s", feat, lex, assoc_freq, assoc_prob, assoc_PMI, assoc_t_test)));
                }
            }
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
//        public double[][] diffMetrix = new double[4][6];
        public double[] distManhattan = new double[4];
        public double[] distEuclidean = new double[4];
        public double[][] simCosine = new double[4][3];
        public double[][] simJaccard = new double[4][2];
        public double[][] simDice = new double[4][2];
        public double[][] simJS = new double[4][2];

        /**
         *              distManhattan   distEuclidean   simCosine   simJaccard  simDice  simJS
         * assoc_freq
         * assoc_prob
         * assoc_PMI
         * assoc_t_test
         */

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Split the key into words
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
                } else {
                    if (lastVal[1].equals(w1)) { // Incomplete pair
                        calculateDiff(lastVal, null);
                    } else {
                        calculateDiff(null, lastVal);
                    }
                    lastVal = parts;
                }
            }

            // Handle the last value if needed (in case of an incomplete pair)
            if (lastVal != null) {
                if (lastVal[1].equals(w1)) {
                    calculateDiff(lastVal, null);
                } else {
                    calculateDiff(null, lastVal);
                }
            }

            // Final calculations

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
//            simDice[i][1] +=
        }

        private void handleSimJS(int i, double val1, double val2){

        }
    }



    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 4 started!");

        String jarBucketName = "classifierinfo";

        String s3InputPath = "s3a://" + jarBucketName + "/counters";
        FileSystem fs = FileSystem.get(URI.create(s3InputPath), new Configuration());
        String L = null;
        String F = null;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(s3InputPath))))) {
            // Read the first line only
            String line = reader.readLine();
            while (line != null) {
                if (line.startsWith("L") || line.startsWith("F")) {
                    String[] parts = line.split(" ");
                    if (parts[0].equals("L")) {
                        L = parts[1];
                    } else {
                        F = parts[1];
                    }
                }
                line = reader.readLine();
            }
        }

        if (L == null || F == null) {
            throw new RuntimeException("Total counters haven't been found!");
        }

        Configuration conf = new Configuration();
        conf.set("L", L);
        conf.set("F", F);

        Job job = Job.getInstance(conf, "Step3");

//        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", 64 * 1024 * 1024); // 64MB (default is 128MB)

        job.setJarByClass(Step3.class);
        job.setMapperClass(Step3.MapperClass.class);
        job.setReducerClass(Step3.ReducerClass.class);
//        job.setCombinerClass(Step3.ReducerClass.class);
//        job.setPartitionerClass(Step3.PartitionerClass.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("s3://" + jarBucketName + "/step2_output/"));

        FileOutputFormat.setOutputPath(job, new Path("s3://" + jarBucketName + "/step3_output/"));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
