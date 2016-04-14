package index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexReducer extends
		Reducer<IndexKey, ArrayWritable, OutputIndexKey, ArrayWritable> {
	@Override
	public void reduce(IndexKey key, Iterable<ArrayWritable> values,
			Context context) throws IOException, InterruptedException {
		List<ArrayWritable> freqs = new ArrayList<ArrayWritable>();

		for (ArrayWritable occurrences : values) {
			freqs.add(occurrences);
		}

		ArrayWritable[] resultArray = new ArrayWritable[freqs.size()];
		context.write(
				new OutputIndexKey(key.getTerm(), freqs.size()),
				new ArrayWritable(ArrayWritable.class, freqs
						.toArray(resultArray)));
	}
}
