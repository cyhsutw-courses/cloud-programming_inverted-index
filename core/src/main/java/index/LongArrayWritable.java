package index;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;

public class LongArrayWritable extends ArrayWritable {

	public LongArrayWritable() {
		super(LongWritable.class);
	}

	public LongArrayWritable(LongWritable[] values) {
		super(LongWritable.class, values);
	}

	@Override
	public String toString() {
		return String.join("", "[",
				String.join(", ", Arrays.asList(this.toStrings())), "]");
	}
}
