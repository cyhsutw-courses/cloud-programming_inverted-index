package index;

import org.apache.hadoop.io.ArrayWritable;

public class TermFrequencyArrayWritable extends ArrayWritable {

	public TermFrequencyArrayWritable() {
		super(TermFrequencyWritable.class);
	}

	public TermFrequencyArrayWritable(TermFrequencyWritable[] values) {
		super(TermFrequencyArrayWritable.class, values);
	}

}
