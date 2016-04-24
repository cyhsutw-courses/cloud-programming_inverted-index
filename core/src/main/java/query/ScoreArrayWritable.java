package query;

import org.apache.hadoop.io.ArrayWritable;

public class ScoreArrayWritable extends ArrayWritable {

	public ScoreArrayWritable() {
		super(ScoreWritable.class);
	}

	public ScoreArrayWritable(ScoreWritable[] values) {
		super(ScoreWritable.class, values);
	}

	@Override
	public String toString() {
		String[] strings = super.toStrings();
		return String.join(";", strings);
	}
}
