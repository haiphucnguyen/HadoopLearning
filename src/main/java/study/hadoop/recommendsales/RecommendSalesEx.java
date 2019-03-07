package study.hadoop.recommendsales;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import study.hadoop.common.ClientRunnerBuilder;
import study.hadoop.prf.PairRelativeFrequency;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class RecommendSalesEx {

    private static class UserCatalogWritable implements Writable, WritableComparable<UserCatalogWritable> {

        private int customerId;
        private int catalogId;

        UserCatalogWritable() {

        }

        UserCatalogWritable(int customerId, int catalogId) {
            this.customerId = customerId;
            this.catalogId = catalogId;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, customerId + ":" + catalogId);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            String value = Text.readString(in);
            String[] arr = value.split(":");
            customerId = Integer.parseInt(arr[0]);
            catalogId = Integer.parseInt(arr[1]);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof UserCatalogWritable)) return false;
            UserCatalogWritable that = (UserCatalogWritable) o;
            return customerId == that.customerId &&
                    catalogId == that.catalogId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(customerId, catalogId);
        }

        @Override
        public int compareTo(UserCatalogWritable o) {
            int result = customerId - o.customerId;
            return (result != 0) ? result : (catalogId - o.catalogId);
        }

        public String toString() {
            return String.format("[%d %d]", customerId, catalogId);
        }
    }

    private static class Pair implements Comparable<Pair> {
        private int catalogId, secondCounts;

        Pair(int catalogId, int secondCounts) {
            this.catalogId = catalogId;
            this.secondCounts = secondCounts;
        }

        @Override
        public int compareTo(Pair o) {
            return (secondCounts - o.secondCounts);
        }
    }

    private static class ToStringArrayWritable extends ArrayWritable {
        ToStringArrayWritable(Class<? extends Writable> valueClass, Writable[] values) {
            super(valueClass, values);
        }

        public String toString() {
            String[] strings = super.toStrings();
            StringBuilder result = new StringBuilder("[");
            for (int i = 0; i < strings.length; i++) {
                result.append(strings[i]);
                if (i < strings.length - 1) {
                    result.append(", ");
                }
            }
            return result.append("]").toString();
        }
    }

    private static class RSMapper extends Mapper<LongWritable, Text, UserCatalogWritable, IntWritable> {

        // Use in combiner method. In the real product, you can have the out of memory error. To prevent this,
        // if the map keys number exceed the x value then you emit the exist elements and clear the map.
        // However, in this demo, I don't care about the memory issue and its solution
        private Map<UserCatalogWritable, Integer> h;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            h = new HashMap<>();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Assume all data is valid
            String[] arr = line.split("[\\s]+");
            int customerId = Integer.parseInt(arr[0]);
            int catalogId = Integer.parseInt(arr[1]);
            int seconds = Integer.parseInt(arr[2]);
            if (seconds > 30) {
                UserCatalogWritable userCatalog = new UserCatalogWritable(customerId, catalogId);
                Integer count = h.get(userCatalog);
                if (count == null) {
                    h.put(userCatalog, seconds);
                } else {
                    h.put(userCatalog, count + seconds);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (UserCatalogWritable userCatalog : h.keySet()) {
                context.write(userCatalog, new IntWritable(h.get(userCatalog)));
            }
        }
    }

    private static class RSReducer extends Reducer<UserCatalogWritable, IntWritable, IntWritable, ToStringArrayWritable> {
        private int currentCustomerId = -1;
        private int currentCatalogId = -1;
        private int currentSeconds = -1;

        private ArrayList<Pair> currentTopThree = null;

        @Override
        protected void reduce(UserCatalogWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int seconds = getSumValue(values);
            if (currentCustomerId == -1) {
                currentCustomerId = key.customerId;
                currentCatalogId = key.catalogId;
                currentSeconds = seconds;
                currentTopThree = new ArrayList<>();
                currentTopThree.add(new Pair(currentCatalogId, currentSeconds));
            } else {
                if (key.customerId != currentCustomerId) {
                    IntWritable[] validCatalogsId = filterValidArrayElement(currentTopThree);

                    // Write customer id only if that customer has at least one product catalog recommendation
                    if (validCatalogsId.length > 0) {
                        context.write(new IntWritable(currentCustomerId), new ToStringArrayWritable(IntWritable.class, validCatalogsId));
                    }
                    currentCustomerId = key.customerId;
                    currentCatalogId = key.catalogId;
                    currentSeconds = seconds;
                    currentTopThree = new ArrayList<>();
                } else {
                    if (key.catalogId != currentCatalogId) {
                        // check if the catalog id is in the current top 3
                        Pair candidate = new Pair(currentCatalogId, currentSeconds);
                        if (currentTopThree.size() < 3) {
                            currentTopThree.add(candidate);
                        } else {
                            Pair lowestInTopThree = currentTopThree.get(0);
                            if (candidate.compareTo(lowestInTopThree) > 0) {
                                currentTopThree.remove(lowestInTopThree);
                                currentTopThree.add(candidate);
                                Collections.sort(currentTopThree);
                            }
                        }

                        currentCatalogId = key.catalogId;
                        currentSeconds = seconds;
                    } else {
                        currentSeconds += seconds;
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (currentCustomerId != -1) {
                IntWritable[] validCatalogsId = filterValidArrayElement(currentTopThree);

                // Write customer id only if that customer has at least one product catalog recommendation
                if (validCatalogsId.length > 0) {
                    context.write(new IntWritable(currentCustomerId), new ToStringArrayWritable(IntWritable.class, validCatalogsId));
                }
            }
        }

        private int getSumValue(Iterable<IntWritable> values) {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            return sum;
        }

        private IntWritable[] filterValidArrayElement(ArrayList<Pair> arrayList) {
            IntWritable[] result = new IntWritable[arrayList.size()];
            for (int i = 0; i < result.length; i++) {
                result[i] = new IntWritable(arrayList.get(i).catalogId);
            }
            return result;
        }
    }

    public static void main(String[] args) throws Exception {
        ClientRunnerBuilder.Client client = new ClientRunnerBuilder(PairRelativeFrequency.class, "sales-recommendation")
                .withOutputKeyClass(UserCatalogWritable.class)
                .withOutputValueClass(IntWritable.class)
                .withMapperClass(RSMapper.class)
                .withReducerClass(RSReducer.class)
                .build();

        client.run("recommend-sales");
    }
}
