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
		System.out.println(key.getTerm() + " : " + key.getDocName());
		for (TermFrequencyWritable freq : values) {
			System.out.println("\t" + freq.getDocName());
			for (Long i : freq.getOffsets()) {
				offsets.add(i);
			}
		}
		offsets.sort(null);

		context.write(key, new TermFrequencyWritable(key.getDocName(), offsets));
		System.out.println("===============================");
	}
}
