package index;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class TermFrequencyArrayWritable extends ArrayWritable {

	public TermFrequencyArrayWritable() {
		super(TermFrequencyWritable.class);
	}

	public TermFrequencyArrayWritable(TermFrequencyWritable[] values) {
		super(TermFrequencyArrayWritable.class, values);
	}

	@Override
	public String toString() {
		for (Writable r : this.get()) {
			System.out.println(((TermFrequencyWritable) r).getDocName());
		}
		return String.join("; ", Arrays.asList(super.toStrings()));
	}
}
