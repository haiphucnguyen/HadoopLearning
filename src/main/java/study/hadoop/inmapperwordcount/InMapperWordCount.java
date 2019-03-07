package study.hadoop.inmapperwordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import study.hadoop.common.ClientRunnerBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class InMapperWordCount {
    public static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Map<String, Integer> h;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            h = new HashMap<>();
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                Integer count = h.get(word);
                if (count == null) {
                    h.put(word, 1);
                } else {
                    h.put(word, count+1);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String word: h.keySet()) {
                context.write(new Text(word), new IntWritable(h.get(word)));
            }
        }
    }

    public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        ClientRunnerBuilder.Client client = new ClientRunnerBuilder(InMapperWordCount.class,"in-mapper-wordcount")
                .withOutputKeyClass(Text.class)
                .withOutputValueClass(IntWritable.class)
                .withMapperClass(WCMapper.class)
                .withReducerClass(WCReducer.class)
                .withKeepOutputFolder(false)
                .build();

        client.run("wordcount");
    }
}
