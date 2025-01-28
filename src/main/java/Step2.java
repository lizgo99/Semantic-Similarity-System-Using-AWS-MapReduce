import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Step2 {
    ///
    /// input: <key, value>:  key = lineID
    ///                       value = l word    count
    ///                       or value = f feature_word-dep_label   count
    ///                       or value = lf word feature_word-dep_label   count
    ///
    /// output: <key, value>:  key = word lf , value = lf word feature count
    ///                        or key = feature lf , value = lf word feature count
    ///                        or key = word l, value = l count
    ///                        or key = word f, value = f count
    ///
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] fields = line.toString().split("\\s+");
            if (fields.length < 3) {
                return;
            }
            String type = fields[0];
            if (fields[0].equals("lf") && fields.length > 3) {
                context.write(new Text(String.format("%s %s", fields[1], type)), // word (lexeme) | type lf
                        new Text(String.format("lf %s %s %s", fields[1], fields[2], fields[3]))); // word (lexeme) | feature | count

                context.write(new Text(String.format("%s %s", fields[2], type)), // feature | type lf
                        new Text(String.format("lf %s %s %s", fields[1], fields[2], fields[3]))); // word (lexeme) | feature | count
            } else {
                context.write(new Text(String.format("%s %s", fields[1], type)), // word (lexeme) | type l or f
                        new Text(String.format("%s %s", type, fields[2]))); // type | count
            }
        }
    }


    ///
    /// input: <key, value>:  key = word lf , value = lf word feature count
    ///                        or key = feature lf , value = lf word feature count
    ///                        or key = word l, value = l count
    ///                        or key = word f, value = f count
    ///
    /// output: <key, value>: key = word feature,
    ///                       value = lf=count f=keywordCount
    ///                       or value = lf=count l=keywordCount
    ///
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        public String keywordType = null;
        public String keywordCount = null;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text value : values) {
                String[] fields = value.toString().split(" ");

                if (fields.length < 2) {
                    continue;
                }

                if (fields[0].equals("l") || fields[0].equals("f")) {
                    keywordType = fields[0];
                    keywordCount = fields[1];
                } else if (fields.length > 2) {
                    String k = String.format("%s %s", fields[1], fields[2]);
                    String v = String.format("lf=%s %s=%s", fields[3], keywordType, keywordCount);
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

        String jarBucketName = args[1];
        String inputPath = args[2];
        String outputPath = args[3];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step2");

        job.setJarByClass(Step2.class);
        job.setMapperClass(Step2.MapperClass.class);
        job.setReducerClass(Step2.ReducerClass.class);
        job.setPartitionerClass(Step2.PartitionerClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath + "part-r*"));

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
