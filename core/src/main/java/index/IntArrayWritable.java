package index;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class IntArrayWritable extends ArrayWritable {

	public IntArrayWritable() {
		super(IntWritable.class);
	}

	public IntArrayWritable(IntWritable[] values) {
		super(IntWritable.class, values);
	}

	@Override
	public String toString() {
		return String.join("", "[",
				String.join(", ", Arrays.asList(this.toStrings())), "]");
	}
}
