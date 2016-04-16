package index;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class IntArrayWritable extends ArrayWritable {

	public IntArrayWritable() {
		super(IntWritable.class);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for (String string : super.toStrings()) {
			builder.append(string).append(" ");
		}
		return builder.toString();
	}
}
