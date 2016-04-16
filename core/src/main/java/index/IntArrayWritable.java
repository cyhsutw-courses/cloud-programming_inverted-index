package index;

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
		StringBuilder builder = new StringBuilder();

		for (String str : super.toStrings()) {
			builder.append(str).append(" ");
		}
		return builder.toString();
	}
}
