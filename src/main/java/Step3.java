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

public class Step3 {
    ///
    /// input: ?
    /// output: ?
    ///
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {

            String[] fields = line.toString().split("\\s+");
            if (fields.length != 4){
                return;
            }
            String w1 = fields[0];
            String w2 = fields[1];
            String lf = fields[2];  // lf=%s
            String f_or_l = fields[3];  // l=%s or f=%s

            context.write(new Text(String.format("%s %s", w1, w2)) ,new Text(String.format("%s %s", lf, f_or_l)));



        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        public double L;
        public double F;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            L = Double.parseDouble(context.getConfiguration().get("L"));
            F = Double.parseDouble(context.getConfiguration().get("F"));
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double l = 0;
            double f = 0;
            double lf = 0;

            for (Text value : values){
                String[] fields = value.toString().split(" ");
                if (fields.length != 2){
                    return;
                }
                lf = Double.parseDouble(fields[0].split("=")[1]);
                String[] l_or_f = fields[1].split("=");
                if(l_or_f[0].equals("l")){
                    l = Double.parseDouble(l_or_f[1]);
                } else if (l_or_f[0].equals("f")) {
                    f = Double.parseDouble(l_or_f[1]);
                }

            }

            if (l != 0 && f != 0 && lf != 0) {
                // Vector assoc_freq (5)
                double assoc_freq = lf;

                // Vector assoc_prob (6)
                double assoc_prob = lf / l;

                // Vector assoc_PMI (7)
                double assoc_PMI = (F * lf) / (l * f);

                // Vector assoc_t_test (8)
                double assoc_t_test = ((lf/L) - (l/L * f/F)) / (Math.sqrt(l/L * f/F));

                context.write(key, new Text(String.format("assoc_freq=%.3f assoc_prob=%.3f assoc_PMI=%.3f assoc_t_test=%.3f", assoc_freq, assoc_prob, assoc_PMI, assoc_t_test)));
            } else {
                context.write(key, new Text(String.format("Error! l=%.3f f=%.3f lf=%.3f", l , f, lf)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");

        String jarBucketName = "classifierinfo1";

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
