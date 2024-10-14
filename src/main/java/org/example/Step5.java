package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.IOException;
public class Step5 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            //Option1 - line[0] = decade, line [1] = sum of npmi
            //Option2 - line[0] = decade, line [1] = w1, line[2] = w2, line[3] = npmi
            String[] line = value.toString().split("\t");
            if(line.length ==2){
                String decade = line[0];
                String sum = line[1];
                context.write(new Text(decade), new Text(sum));
                return;
            }
            String decade = line[0];
            String w1 = line[1];
            String w2 = line[2];
            String npmi = line[3];
            context.write(new Text(decade + "\t" + npmi), new Text(w1 + "\t" + w2));
        }
    }


    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        double minPmi = -1;
        double relMinPmi = -1;
        double currentDecadeNPMISum = -1;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            minPmi = Double.parseDouble(conf.get("minPmi"));
            relMinPmi = Double.parseDouble(conf.get("relMinPmi"));
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            String[] keys = key.toString().split("\t");
            if(keys.length == 1){
                for(Text value : values){
                    currentDecadeNPMISum = Double.parseDouble(value.toString());
                    return;
                }
            }
            String decade = keys[0];
            String npmi = keys[1];
            for(Text value : values){
                String[] w1w2 = value.toString().split("\t");
                String w1 = w1w2[0];
                String w2 = w1w2[1];
                double npmiValue = Double.parseDouble(npmi);
                if(npmiValue >= minPmi || npmiValue / currentDecadeNPMISum >= relMinPmi){
                    context.write(new Text(decade + "\t" + w1 + "\t" + w2), new Text(npmi));
                }
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            // getPartition is acording to the decade
            String[] keys = key.toString().split("\t");
            return Math.abs(keys[0].hashCode() % numPartitions);
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 5 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
//        conf.set("mapred.max.split.size", String.valueOf(25 * 1024 * 1024)); // adjustment of the split size -> increase/decrease the number of mappers
        conf.set("minPmi", args[1]);
        conf.set("relMinPmi", args[2]);
        Job job = Job.getInstance(conf, "Step 5");
        job.setJarByClass(Step5.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(Step5.PartitionerClass.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
        job.setOutputFormatClass(TextOutputFormat.class);
//       job.setInputFormatClass(SequenceFileInputFormat.class);
        //    TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/2gram/data"));
        FileInputFormat.addInputPath(job, new Path("s3://bucket1638974297771/OUT/output_step4"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket1638974297771/OUT/output_step5"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
