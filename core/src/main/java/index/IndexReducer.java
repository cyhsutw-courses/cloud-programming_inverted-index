package index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.mapreduce.Reducer;

public class IndexReducer
		extends
		Reducer<IndexKey, TermFrequencyWritable, OutputIndexKey, TermFrequencyArrayWritable> {
	@Override
	public void reduce(IndexKey key, Iterable<TermFrequencyWritable> values,
			Context context) throws IOException, InterruptedException {
		System.out.println(context.getConfiguration().get("numnum"));
		List<TermFrequencyWritable> freqs = new ArrayList<>();

		for (TermFrequencyWritable freq : values) {
			freqs.add(new TermFrequencyWritable(freq.getDocName(), Arrays
					.asList(freq.getOffsets())));
		}

		TermFrequencyWritable[] resultArray = new TermFrequencyWritable[freqs
				.size()];
		context.write(new OutputIndexKey(key.getTerm(), freqs.size()),
				new TermFrequencyArrayWritable(freqs.toArray(resultArray)));
	}
}
