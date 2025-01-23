import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.net.URI;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import org.apache.hadoop.fs.FileSystem;

public class Step1 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        ///
        /// Counts all the valuable data in each line
        ///
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {

            String[] fields = line.toString().split("\t"); //head_word<TAB>syntactic-ngram<TAB>total_count<TAB>counts_by_year

            if (fields.length < 4) { // Unknown format
                return;
            }

            String count = fields[2];
            String[] words = fields[1].split(" ");

            String[][] parts = new String[words.length][4];
            // Replace each word with it's stemmed version
            for (int i = 0; i < words.length; i++){
                String word = words[i];
                Stemmer stemmer = new Stemmer();
                String oldWord = word.substring(0, word.indexOf("/"));
                boolean fullyStemmed = true;
                for (char c : oldWord.toCharArray()){
                    if (Character.isLetter(c)) {
                        stemmer.add(c);
                    } else {
                        fullyStemmed = false;
                        break;
                    }
                }
                if (fullyStemmed){
                    stemmer.stem();
                    words[i] = word.replace(oldWord, stemmer.toString());
                }
                parts[i] = words[i].split("/");
            }

            for (String[] word : parts) {
                int pointer = Integer.parseInt(word[3]);  // e.g. for/IN/prep/1
                if (pointer == 0) {
                    continue;
                }

                // Count lexeme
                String lex = parts[pointer - 1][0];
                context.write(new Text(String.format("l %s", lex)), new Text(count));
                // Count feature
                String feat = word[0] + "-" +word[2];
                context.write(new Text(String.format("f %s", feat)), new Text(count));
                // Count lexeme feature pair
                context.write(new Text(String.format("lf %s %s", lex, feat)), new Text(count));
                // Add to the total count of features and lexemes
                context.getCounter("TotalCounters", "L").increment(Integer.parseInt(count));
                context.getCounter("TotalCounters", "F").increment(Integer.parseInt(count));
            }
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int acc = 0;
            for (Text value: values){
                acc += Integer.parseInt(value.toString());
            }
            context.write(key, new Text(String.format("%d", acc)));
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");

        String jarBucketName = "classifierinfo1";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step1");

//        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", 64 * 1024 * 1024); // 64MB (default is 128MB)

        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setCombinerClass(ReducerClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("s3://" + jarBucketName + "/input-samples/"));



//        MultipleOutputs.addNamedOutput(job, "1gram", TextOutputFormat.class, Text.class, Text.class);
//        MultipleOutputs.addNamedOutput(job, "2gram", TextOutputFormat.class, Text.class, Text.class);
//        MultipleOutputs.addNamedOutput(job, "3gram", TextOutputFormat.class, Text.class, Text.class);
//        MultipleOutputs.addNamedOutput(job, "error", TextOutputFormat.class, Text.class, Text.class);


        FileOutputFormat.setOutputPath(job, new Path("s3://" + jarBucketName + "/step1_output/"));

        boolean success = job.waitForCompletion(true);

        // Write down Totals values to use in the next step
        if (success) {

            String countersOutput = "s3://" + jarBucketName  + "/counters";
            // Open file
            FileSystem fs = FileSystem.get(URI.create(countersOutput), new Configuration());

            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(fs.create(new Path(countersOutput), true)))) {
                // Write each counter
                for (Counter c : job.getCounters().getGroup("TotalCounters")){
                    String name = c.getName();
                    long value = c.getValue();

                    writer.write(String.format("%s %d\n", name, value));
                }
            }
        }
        System.exit(success ? 0 : 1);
    }

}
