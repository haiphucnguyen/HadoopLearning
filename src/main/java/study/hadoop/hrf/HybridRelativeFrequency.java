package study.hadoop.hrf;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import study.hadoop.common.ClientRunnerBuilder;
import study.hadoop.common.ToStringMapWritable;
import study.hadoop.common.PairWritable;

import java.io.IOException;

public class HybridRelativeFrequency {

    private static class HRFMapper extends Mapper<LongWritable, Text, PairWritable, IntWritable> {

        private ToStringMapWritable h;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            h = new ToStringMapWritable();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split("[\\s+|,]+");
            for (int i = 0; i < items.length; i++) {
                for (int j = i + 1; j < items.length; j++) {
                    if (!items[i].equals(items[j])) {
                        PairWritable uv = new PairWritable(items[i], items[j]);
                        IntWritable countVal = (IntWritable) h.get(uv);
                        if (countVal == null) {
                            h.put(uv, new IntWritable(1));
                        } else {
                            countVal.set(countVal.get() + 1);
                            h.put(uv, countVal);
                        }
                    } else break;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Writable uv : h.keySet()) {
                context.write((PairWritable) uv, (IntWritable) h.get(uv));
            }
        }
    }

    private static class HRFReducer extends Reducer<PairWritable, IntWritable, Text, ToStringMapWritable> {

        private String prevU = null;
        private ToStringMapWritable h;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            h = new ToStringMapWritable();
        }

        @Override
        protected void reduce(PairWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            if (prevU != null && !key.getLeft().equals(prevU)) {
                int total = total(h);
                for (Writable mKey : h.keySet()) {
                    IntWritable value = (IntWritable) h.get(mKey);
                    h.put(mKey, new FloatWritable((float) value.get() / total));
                }
                context.write(new Text(prevU), h);
                h.clear();
            }

            prevU = key.getLeft();
            h.put(new Text(key.getRight()), new IntWritable(total(values)));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int total = total(h);
            for (Writable mKey : h.keySet()) {
                IntWritable value = (IntWritable) h.get(mKey);
                h.put(mKey, new FloatWritable((float) value.get() / total));
            }
            context.write(new Text(prevU), h);
        }

        private int total(MapWritable map) {
            int sum = 0;
            for (Writable key : map.keySet()) {
                sum += ((IntWritable) map.get(key)).get();
            }
            return sum;
        }

        private int total(Iterable<IntWritable> iter) {
            int sum = 0;
            for (IntWritable value: iter) {
                sum += value.get();
            }
            return sum;
        }
    }

    public static void main(String[] args) throws Exception {
        ClientRunnerBuilder.Client client = new ClientRunnerBuilder(HybridRelativeFrequency.class, "hybrid-relative-frequency")
                .withOutputKeyClass(PairWritable.class)
                .withOutputValueClass(IntWritable.class)
                .withMapperClass(HRFMapper.class)
                .withReducerClass(HRFReducer.class)
                .withKeepOutputFolder(false)
                .build();

        client.run("pairstripe");
    }
}
