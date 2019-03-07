package study.hadoop.prf;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import study.hadoop.common.ClientRunnerBuilder;
import study.hadoop.common.PairWritable;

import java.io.IOException;

public class PairRelativeFrequency {

    private static class PRFMapper extends Mapper<LongWritable, Text, PairWritable, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split("[\\s+|,]+");
            for (int i = 0; i < items.length; i++) {

                for (int j = i + 1; j < items.length; j++) {
                    if (!items[i].equals(items[j])) {
                        context.write(new PairWritable(items[i], items[j]), new IntWritable(1));
                        context.write(new PairWritable(items[i], "0"), new IntWritable(1));
                    } else break;
                }
            }
        }
    }

    private static class PRFReducer extends Reducer<PairWritable, IntWritable, PairWritable, FloatWritable> {

        private int sum;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            sum = 0;
        }

        @Override
        protected void reduce(PairWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int s = 0;
            for (IntWritable value : values) {
                s += value.get();
                if (key.getRight().equals("0")) {
                    sum = s;
                } else {
                    context.write(key, new FloatWritable((float) s / sum));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ClientRunnerBuilder.Client client = new ClientRunnerBuilder(PairRelativeFrequency.class, "pair-relative-frequency")
                .withOutputKeyClass(PairWritable.class)
                .withOutputValueClass(IntWritable.class)
                .withMapperClass(PRFMapper.class)
                .withReducerClass(PRFReducer.class)
                .withKeepOutputFolder(false)
                .build();

        client.run("pairstripe");
    }
}
