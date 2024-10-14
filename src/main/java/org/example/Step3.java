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
public class Step3 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {


        private Integer c_w1 = -1;


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            //Option1 - line[0] = decade, line [1] = w1, line[2] = c(w1)
            //Option2 - line[0] = decade, line [1] = w1 , line[2] = w2, line[3] = N, line[4] = c(w1,w2)
            String[] line = value.toString().split("\t");

            if(line.length ==3){
                c_w1 = Integer.parseInt(line[2]);
                return;
            }
            String decade = line[0];
            String w1 = line[1];
            String w2 = line[2];
            String numOfBigrams = line[3];
            String c_w1w2 = line[4];

            String Key1 = decade + "\t" + w2;
            context.write(new Text(Key1), new Text(c_w1w2));
            String Key2 = decade + "\t" + w2 + "\t" + w1;
            String outputValue = "" + numOfBigrams + "\t" + c_w1w2 + "\t" + c_w1;
            context.write(new Text(Key2), new Text(outputValue));
        }
    }


    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            String[] keys = key.toString().split("\t");
            if(keys.length == 2){
                //key is in the form of <decade w2>
                int sum = 0;
                for (Text value : values) {
                    sum += Integer.parseInt(value.toString());
                }
                //output: <decade w2, sum>
                context.write(key, new Text(Integer.toString(sum)));
                return;
            }
            //key is in the form of <decade w2 w1>
            for (Text value : values) {
                context.write(key, value);
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
        System.out.println("[DEBUG] STEP 3 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
//        conf.set("mapred.max.split.size", String.valueOf(25 * 1024 * 1024)); // adjustment of the split size -> increase/decrease the number of mappers
        Job job = Job.getInstance(conf, "Step 3");
        job.setJarByClass(Step3.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(Step3.PartitionerClass.class);
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
        FileInputFormat.addInputPath(job, new Path("s3://bucket1638974297771/OUT/output_step2"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket1638974297771/OUT/output_step3"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
