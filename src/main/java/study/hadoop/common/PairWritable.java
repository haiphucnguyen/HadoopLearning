package study.hadoop.common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PairWritable implements Writable, WritableComparable<PairWritable> {
    private String left;
    private String right;

    public PairWritable() {
    }

    public PairWritable(String left, String right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, left + ":" + right);
        Text.writeString(out, right);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String value = Text.readString(in);
        String[] arr = value.split(":");
        left = arr[0];
        right = arr[1];
    }

    public String getLeft() {
        return left;
    }

    public void setLeft(String left) {
        this.left = left;
    }

    public String getRight() {
        return right;
    }

    public void setRight(String right) {
        this.right = right;
    }

    @Override
    public int compareTo(PairWritable o) {
        int result = left.compareTo(o.left);
        if (result == 0) {
            return right.compareTo(o.right);
        } else return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PairWritable)) return false;
        PairWritable that = (PairWritable) o;
        return left.equals(that.left) &&
                right.equals(that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }

    @Override
    public String toString() {
        return String.format("(%s, %s)", left, right);
    }
}
