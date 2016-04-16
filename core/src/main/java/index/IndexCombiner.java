package index;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Reducer;

public class IndexCombiner
		extends
		Reducer<IndexKey, TermFrequencyWritable, IndexKey, TermFrequencyWritable> {

	@Override
	public void reduce(IndexKey key, Iterable<TermFrequencyWritable> values,
			Context context) throws IOException, InterruptedException {

		ArrayList<Long> offsets = new ArrayList<>();
		for (TermFrequencyWritable freq : values) {
			for (Long i : freq.getOffsets()) {
				offsets.add(i);
			}
		}
		offsets.sort(null);

		context.write(key, new TermFrequencyWritable(key.getDocName(), offsets));
	}
}
