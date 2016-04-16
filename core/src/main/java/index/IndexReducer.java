package index;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;

public class IndexReducer
		extends
		Reducer<IndexKey, TermFrequencyWritable, OutputIndexKey, TermFrequencyArrayWritable> {
	@Override
	public void reduce(IndexKey key, Iterable<TermFrequencyWritable> values,
			Context context) throws IOException, InterruptedException {
		List<TermFrequencyWritable> freqs = Lists.newArrayList(values);

		TermFrequencyWritable[] resultArray = new TermFrequencyWritable[freqs
				.size()];
		context.write(new OutputIndexKey(key.getTerm(), freqs.size()),
				new TermFrequencyArrayWritable(freqs.toArray(resultArray)));
	}
}
