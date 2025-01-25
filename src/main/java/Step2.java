import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Step2 {
    ///
    /// input: <key, value> key = l word or key = f feature_word-dep_label or key =
    /// lf word feature_word-dep_label, value = count
    /// output: ?
    ///
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        // private final String NULL_CHARACTER = "\u0000";
        private MultipleOutputs<Text, Text> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<>(context);
        }
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] fields = line.toString().split("\\s+");
            if (fields.length < 3) {
                multipleOutputs.write("debugOutput", new Text("[ERROR] Invalid line format"),
                        new Text("Expected 3 fields, got " + fields.length));
                return;
            }
            String type = fields[0];
            if (fields[0].equals("lf") && fields.length > 3) {
                context.write(new Text(String.format("%s %s", fields[1], type)),
                        new Text(String.format("lf %s %s %s", fields[1], fields[2], fields[3])));
                context.write(new Text(String.format("%s %s", fields[2], type)),
                        new Text(String.format("lf %s %s %s", fields[1], fields[2], fields[3])));
            } else {
                context.write(new Text(String.format("%s %s", fields[1], type)),
                        new Text(String.format("%s %s", type, fields[2])));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        public String keywordType = null;
        public String keywordVal = null;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text value : values) {
                String[] fields = value.toString().split(" ");

                if (fields.length < 2) {
                    continue;
                }

                if (fields[0].equals("l") || fields[0].equals("f")) {
                    keywordType = fields[0];
                    keywordVal = fields[1];
                } else if (fields.length > 2) {
                    String k = String.format("%s %s", fields[1], fields[2]);
                    String v = String.format("lf=%s %s=%s", fields[3], keywordType, keywordVal);
                    context.write(new Text(k), new Text(v));
                }
            }
        }
    }

    ///
    /// Partition by the second word
    ///
    public static class PartitionerClass extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String w = key.toString().split(" ")[0];
            return Math.abs(w.hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");

        // String jarBucketName = "classifierinfo1";

        String jarBucketName = args[1];
        String inputPath = args[2];
        String outputPath = args[3];

        System.out.println("[DEBUG] Input path: " + inputPath);
        System.out.println("[DEBUG] Output path: " + outputPath);
        System.out.println("[DEBUG] Jar bucket name: " + jarBucketName);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step2");

        job.setJarByClass(Step2.class);
        job.setMapperClass(Step2.MapperClass.class);
        job.setReducerClass(Step2.ReducerClass.class);
        // job.setCombinerClass(Step2.ReducerClass.class);
        job.setPartitionerClass(Step2.PartitionerClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath + "part-r*"));

        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        MultipleOutputs.addNamedOutput(job, "debugOutput", TextOutputFormat.class, Text.class, Text.class);


        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
