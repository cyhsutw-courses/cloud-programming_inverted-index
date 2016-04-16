package index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexReducer extends
		Reducer<IndexKey, TermFrequencyArrayWritable, OutputIndexKey, Text> {
	@Override
	public void reduce(IndexKey key,
			Iterable<TermFrequencyArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		List<TermFrequencyArrayWritable> freqs = new ArrayList<>();

		for (TermFrequencyArrayWritable occurrences : values) {
			freqs.add(occurrences);
		}

		// TermFrequencyArrayWritable[] resultArray = new
		// TermFrequencyArrayWritable[freqs.size()];
		context.write(new OutputIndexKey(key.getTerm(), freqs.size()),
				new Text("OK"));
	}
}
