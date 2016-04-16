package index;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntArrayWritable extends ArrayWritable {

	public IntArrayWritable() {
		super(IntWritable.class);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		for (Writable wt : this.get()) {
			builder.append(wt.toString()).append(" ");
		}
		return builder.toString();
	}
}
