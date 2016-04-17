package index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;

public class LongArrayWritable extends ArrayWritable {

	public LongArrayWritable() {
		super(LongWritable.class);
	}

	public LongArrayWritable(LongWritable[] values) {
		super(LongWritable.class, values);
	}

	public List<Long> asList() {
		List<Long> list = new ArrayList<>();
		String[] values = this.toStrings();
		for (String s : values) {
			list.add(Long.parseLong(s));
		}
		return list;
	}

	@Override
	public String toString() {
		return String.join("", "[",
				String.join(",", Arrays.asList(this.toStrings())), "]");
	}
}
