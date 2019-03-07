package study.hadoop.inmapperava;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import study.hadoop.common.ClientRunnerBuilder;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMapperAverageComputationalAlgorithm {
    static class ACAMapper extends Mapper<LongWritable, Text, Text, ArrayPrimitiveWritable> {

        private Map<String, List<Integer>> h;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            h = new HashMap<>();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] arr = line.split(" ");
            String ip = arr[0];
            try {
                int quantity = Integer.parseInt(arr[arr.length - 1]);
                if (h.get(ip) != null) {
                    h.get(ip).add(quantity);
                } else {
                    List<Integer> list = new ArrayList<>();
                    list.add(quantity);
                    h.put(ip, list);
                }
            } catch (Exception e) {
                // Invalid format such as 80-219-148-207.dclient.hispeed.ch - - [07/Mar/2004:19:47:36 -0800] "OPTIONS * HTTP/1.0" 200 -
                // Do nothing
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String ip : h.keySet()) {
                List<Integer> values = h.get(ip);
                values.toArray(new Integer[0]);
                int[] arr = new int[values.size()];
                for (int i = 0; i < arr.length; i++) {
                    arr[i] = values.get(i);
                }
                context.write(new Text(ip), new ArrayPrimitiveWritable(arr));
            }
        }
    }

    static class ACAReducer extends Reducer<Text, ArrayPrimitiveWritable, Text, FloatWritable> {
        @Override
        protected void reduce(Text key, Iterable<ArrayPrimitiveWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0L;
            int count = 0;
            for (ArrayPrimitiveWritable valueWrapper : values) {
                Object value = valueWrapper.get();
                int length = Array.getLength(value);
                count += length;
                for (int i = 0; i < length; i++) {
                    sum += Array.getInt(value, i);
                }
            }
            float average = (float) sum / count;
            context.write(key, new FloatWritable(average));
        }
    }

    public static void main(String[] args) throws Exception {
        ClientRunnerBuilder.Client client = new ClientRunnerBuilder(InMapperAverageComputationalAlgorithm.class, "average-computational-algorithm")
                .withOutputKeyClass(Text.class)
                .withOutputValueClass(ArrayPrimitiveWritable.class)
                .withMapperClass(ACAMapper.class)
                .withReducerClass(ACAReducer.class)
                .withKeepOutputFolder(false)
                .build();
        client.run("logs");
    }
}
