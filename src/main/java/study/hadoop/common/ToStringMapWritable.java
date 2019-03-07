package study.hadoop.common;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class ToStringMapWritable extends MapWritable {
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder().append("[");
        for (Writable key : keySet()) {
            builder.append(key.toString() + ": " + get(key).toString()).append(", ");
        }
        return builder.delete(builder.length() - 2, builder.length() - 1).append("]").toString();
    }
}
