package index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexCombiner extends
		Reducer<IndexKey, ArrayWritable, IndexKey, ArrayWritable> {

	@Override
	public void reduce(IndexKey key, Iterable<ArrayWritable> values,
			Context context) throws IOException, InterruptedException {

		System.out.println(key.getTerm() + ":" + key.getDocName());

		ArrayList<TermFrequencyWritable> offsets = new ArrayList<>();

		for (ArrayWritable freqs : values) {
			TermFrequencyWritable[] freqArr = (TermFrequencyWritable[]) freqs
					.toArray();
			ArrayList<Integer> allOffsets = new ArrayList<>();
			for (TermFrequencyWritable freq : freqArr) {
				System.out.println("\t" + freq.getDocName());
				Integer[] someOffsets = freq.getOffsets();
				allOffsets.addAll(Arrays.asList(someOffsets));
			}
			allOffsets.sort(null);
			offsets.add(new TermFrequencyWritable(freqArr[0].getDocName(),
					allOffsets));
		}

		TermFrequencyWritable[] arr = new TermFrequencyWritable[offsets.size()];
		context.write(key, new ArrayWritable(TermFrequencyWritable.class,
				offsets.toArray(arr)));
	}
}
