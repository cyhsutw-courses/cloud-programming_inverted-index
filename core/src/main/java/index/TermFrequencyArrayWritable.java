package index;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;

public class TermFrequencyArrayWritable extends ArrayWritable {

	public TermFrequencyArrayWritable() {
		super(TermFrequencyWritable.class);
	}

	public TermFrequencyArrayWritable(TermFrequencyWritable[] values) {
		super(TermFrequencyArrayWritable.class, values);
	}

	@Override
	public String toString() {
		return String.join("; ", Arrays.asList(super.toStrings()));
	}
}
