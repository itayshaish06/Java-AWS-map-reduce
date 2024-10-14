package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.concurrent.ConcurrentHashMap;

import java.io.IOException;
public class Step1 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

        // concurent hashmap for stop words
        private ConcurrentHashMap<String, Integer> stopWordsCorpus = new ConcurrentHashMap<String, Integer>();

    //hashmap for stop words
    //override setup method to read the stop words file and store them in the hashmap
        protected void Setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException{
            super.setup(context);
//            String[] stopWordsInput = {"a", "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost",
//                        "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst",
//                        "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are",
//                        "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before",
//                        "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both",
//                        "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "computer", "con", "could", "couldnt", "cry",
//                        "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven",
//                        "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere",
//                        "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly",
//                        "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt",
//                        "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him",
//                        "himself", "his", "how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is",
//                        "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me",
//                        "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself",
//                        "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor",
//                        "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other",
//                        "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps", "please",
//                        "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several",
//                        "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow",
//                        "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system",
//                        "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there",
//                        "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin",
//                        "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to",
//                        "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under",
//                        "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever",
//                        "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein",
//                        "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole",
//                        "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your",
//                        "yours", "yourself", "yourselves"};
            String[] stopWordsInput = {
                    "״", "׳", "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן", "מכל", "מי", "מהם", "מה", "מ", "למה", "לכל", "לי", "לו",
                    "להיות", "לה", "לא", "כן", "כמה", "כלי", "כל", "כי", "יש", "ימים", "יותר", "יד", "י", "זה", "ז", "ועל", "ומי", "ולא", "וכן",
                    "וכל", "והיא", "והוא", "ואם", "ו", "הרבה", "הנה", "היו", "היה", "היא", "הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו", "בה",
                    "בא", "את", "אשר", "אם", "אלה", "אל", "אך", "איש", "אין", "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":", "1", ".",
                    "-", "*", "\"", "!", "שלשה", "בעל", "פני", ")", "גדול", "שם", "עלי", "עולם", "מקום", "לעולם", "לנו", "להם", "ישראל", "יודע", "זאת",
                    "השמים", "הזאת", "הדברים", "הדבר", "הבית", "האמת", "דברי", "במקום", "בהם", "אמרו", "אינם", "אחרי", "אותם", "אדם", "(", "חלק", "שני",
                    "שכל", "שאר", "ש", "ר", "פעמים", "נעשה", "ן", "ממנו", "מלא", "מזה", "ם", "לפי", "ל", "כמו", "כבר", "כ", "זו", "ומה", "ולכל", "ובין",
                    "ואין", "הן", "היתה", "הא", "ה", "בל", "בין", "בזה", "ב", "אף", "אי", "אותה", "או", "אבל", "א"
            };
                for (String word : stopWordsInput)
                    stopWordsCorpus.put(word, 0);
        }

            /*
            * Input:
            * 1) <key, value> = <line number, lineText>
            * lineText = <w1 w2   year    occurrences    ignore>
            * Output:
            * 1) <key, value> = <decade, occurrences>
            * 2) <key, value> = <decade'\t'w1 w2, occurrences>
            */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] line = value.toString().split("\t");
            String[] words = line[0].split(" ");
            if(line.length < 4 | words.length != 2 || words[0].length() == 1 || words[1].length() == 1 ) {
                return;
            }
            String w1 = words[0];
            String w2 = words[1];
            if(stopWordsCorpus.containsKey(w1) || stopWordsCorpus.containsKey(w2)) {
                return;
            }
            IntWritable occurrences = new IntWritable(Integer.parseInt(line[2]));
            int year = Integer.parseInt(line[1]);
            String decade = Integer.toString(year - year % 10);
            context.write(new Text(decade), occurrences);
            context.write(new Text(decade + "\t" + w1 + " " + w2), occurrences);
        }
    }

    /*
    * Input:
    * 1) <key, value> = <decade, occurrences>
    * 2) <key, value> = <decade'\t'w1 w2, occurrences>
    * Output:
    * 1) <key, value> = <decade, sum of bi-grams>
    * 2) <key, value> = <decade'\t'w1 w2, sum of occurrences>
    *
    */
    public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
    }
}

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            // getPartition is acording to the decade
            String[] keys = key.toString().split("\t");
            return Math.abs(keys[0].hashCode() % numPartitions);
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
//        conf.set("mapred.max.split.size", String.valueOf(25 * 1024 * 1024)); // adjustment of the split size -> increase/decrease the number of mappers
        Job job = Job.getInstance(conf, "Step 1");
        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(Step1.PartitionerClass.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
       job.setOutputFormatClass(TextOutputFormat.class);
       job.setInputFormatClass(SequenceFileInputFormat.class);
//     TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/2gram/data")); // english biggest file
//     TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-1M/2gram/data")); // english smaller file
       TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data")); // hebrew file

//        s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data
//        FileInputFormat.addInputPath(job, new Path("s3://bucket1638974297771/googlebooks-eng-all-2gram-20120701-0 - Copy2"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket1638974297771/OUT/output_step1"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
