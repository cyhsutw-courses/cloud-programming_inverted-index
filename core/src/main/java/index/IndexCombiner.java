package index;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexCombiner extends
		Reducer<IndexKey, TermFrequencyWritable, Text, TermFrequencyWritable> {

	@Override
	public void reduce(IndexKey key, Iterable<TermFrequencyWritable> values,
			Context context) throws IOException, InterruptedException {

		ArrayList<Integer> offsets = new ArrayList<>();

		for (TermFrequencyWritable freq : values) {
			for (Integer i : freq.getOffsets()) {
				offsets.add(i);
			}
		}
		offsets.sort(null);

		context.write(new Text(key.getDocName()),
				new TermFrequencyWritable(key.getDocName(), offsets));
	}
}
