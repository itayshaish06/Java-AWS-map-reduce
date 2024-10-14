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
public class Step4 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {


        private Integer c_w2 = -1;


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            //Option1 - line[0] = decade, line [1] = w2, line[2] = c(w2)
            //Option2 - line[0] = decade, line [1] = w2 , line[2] = w1, line[3] = N, line[4] = c(w1,w2), line[5] = c(w1)
            String[] line = value.toString().split("\t");

            if(line.length ==3){
                c_w2 = Integer.parseInt(line[2]);
                return;
            }
            String decade = line[0];
            String w2 = line[1];
            String w1 = line[2];
            String numOfBigrams = line[3];
            String c_w1w2 = line[4];
            String c_w1 = line[5];

            String key1 = decade;
            String value1 = w1 + "\t" + w2 + "\t" + numOfBigrams + "\t" + c_w1w2 + "\t" + c_w1 + "\t" + c_w2;
            context.write(new Text(key1), new Text(value1));
        }
    }


    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            double sum = 0;
            for(Text value : values){
                String[] valueString = value.toString().split("\t");
                String w1 = valueString[0];
                String w2 = valueString[1];
                double numOfBigrams = Double.parseDouble(valueString[2]);
                double c_w1w2 = Double.parseDouble(valueString[3]);
                double c_w1 = Double.parseDouble(valueString[4]);
                double c_w2 = Double.parseDouble(valueString[5]);
                double pmi = log2(c_w1w2) + log2(numOfBigrams) - log2(c_w1) - log2(c_w2);
                double p_w1w2 = numOfBigrams != 0 ? c_w1w2 / numOfBigrams : 0.00001; // 0.00001 - testing
                double npmi = p_w1w2 != 1 ? pmi / (-1.00 * log2(p_w1w2)) : 0.0;
                if(npmi > 0.95)
                    continue;
                sum += npmi;
                Text newKey = new Text(key.toString() + "\t" + w1 + "\t" + w2);
                context.write(newKey, new Text(Double.toString(npmi)));
            }
            context.write(key, new Text(Double.toString(sum)));
        }

        public static double log2(double x){
            return Math.log(x) / Math.log(2);
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            // getPartition is acording to the decade
            return Math.abs(key.hashCode() % numPartitions);
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 4 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
//        conf.set("mapred.max.split.size", String.valueOf(25 * 1024 * 1024)); // adjustment of the split size -> increase/decrease the number of mappers
        Job job = Job.getInstance(conf, "Step 4");
        job.setJarByClass(Step4.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(Step4.PartitionerClass.class);
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
        FileInputFormat.addInputPath(job, new Path("s3://bucket1638974297771/OUT/output_step3"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket1638974297771/OUT/output_step4"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
