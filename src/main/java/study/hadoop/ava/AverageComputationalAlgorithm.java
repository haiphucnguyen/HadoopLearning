package study.hadoop.ava;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import study.hadoop.common.ClientRunnerBuilder;
import study.hadoop.common.ClientRunnerBuilder.Client;

import java.io.IOException;

public class AverageComputationalAlgorithm {

    static class ACAMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] arr = line.split(" ");
            String ip = arr[0];
            try {
                int quantity = Integer.parseInt(arr[arr.length - 1]);
                context.write(new Text(ip), new IntWritable(quantity));
            } catch (Exception e) {
                // Invalid format such as 80-219-148-207.dclient.hispeed.ch - - [07/Mar/2004:19:47:36 -0800] "OPTIONS * HTTP/1.0" 200 -
                // Do nothing
            }
        }
    }

    static class ACAReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0L;
            int count = 0;
            for (IntWritable value : values) {
                sum += value.get();
                count++;
            }
            context.write(key, new FloatWritable((float) sum / count));
        }
    }

    public static void main(String[] args) throws Exception {
        Client client = new ClientRunnerBuilder(AverageComputationalAlgorithm.class, "average-computational-algorithm")
                .withOutputKeyClass(Text.class)
                .withOutputValueClass(IntWritable.class)
                .withMapperClass(ACAMapper.class)
                .withReducerClass(ACAReducer.class)
                .withKeepOutputFolder(false)
                .build();
        client.run("logs");
    }
}
