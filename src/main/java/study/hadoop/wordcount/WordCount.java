package study.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import study.hadoop.common.ClientRunnerBuilder;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    public static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        ClientRunnerBuilder.Client client = new ClientRunnerBuilder(WordCount.class,"wordcount")
                .withOutputKeyClass(Text.class)
                .withOutputValueClass(IntWritable.class)
                .withMapperClass(WCMapper.class)
                .withReducerClass(WCReducer.class)
                .withKeepOutputFolder(false)
                .build();

        client.run("wordcount");
    }
}