package index;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;

public class TermFrequencyArrayCollectionWritable extends ArrayWritable {

	public TermFrequencyArrayCollectionWritable() {
		super(TermFrequencyArrayWritable.class);
	}

	public TermFrequencyArrayCollectionWritable(
			TermFrequencyArrayWritable[] values) {
		super(TermFrequencyArrayWritable.class, values);
	}

	@Override
	public String toString() {
		return String.join(" ", Arrays.asList(this.toStrings()));
	}
}
