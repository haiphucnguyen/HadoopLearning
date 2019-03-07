package study.hadoop.saf;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import study.hadoop.common.ClientRunnerBuilder;
import study.hadoop.common.ToStringMapWritable;

import java.io.IOException;

public class StripeRelativeFrequency {

    private static class SRFMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split("[\\s+|,]+");
            for (int i = 0; i < items.length; i++) {
                MapWritable mapItemI = new MapWritable();
                for (int j = i + 1; j < items.length; j++) {
                    if (!items[i].equals(items[j])) {
                        mapItemI.put(new Text(items[j]), new IntWritable(1));
                    } else break;
                }
                context.write(new Text(items[i]), mapItemI);
            }
        }
    }

    private static class SRFReducer extends Reducer<Text, MapWritable, Text, ToStringMapWritable> {
        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            ToStringMapWritable result = new ToStringMapWritable();
            int sum = 0;
            for (MapWritable map : values) {
                for (Writable mKey : map.keySet()) {
                    IntWritable mValue = (IntWritable) map.get(mKey);
                    sum += mValue.get();
                    IntWritable rValue = (IntWritable) result.get(mKey);
                    if (rValue == null) {
                        result.put(mKey, mValue);
                    } else {
                        result.put(mKey, new IntWritable(rValue.get() + mValue.get()));
                    }
                }
            }

            for (Writable mKey : result.keySet()) {
                IntWritable value = (IntWritable) result.get(mKey);
                result.put(mKey, new FloatWritable((float) value.get() / sum));
            }
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        ClientRunnerBuilder.Client client = new ClientRunnerBuilder(StripeRelativeFrequency.class, "stripe-relative-frequency")
                .withOutputKeyClass(Text.class)
                .withOutputValueClass(MapWritable.class)
                .withMapperClass(SRFMapper.class)
                .withReducerClass(SRFReducer.class)
                .withKeepOutputFolder(false)
                .build();

        client.run("pairstripe");
    }
}
