package index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexCombiner extends
		Reducer<IndexKey, IntWritable, IndexKey, ArrayWritable> {

	@Override
	public void reduce(IndexKey key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		List<IntWritable> offsets = new ArrayList<IntWritable>();

		for (IntWritable offset : values) {
			offsets.add(offset);
		}

		IntWritable[] resultArray = new IntWritable[offsets.size()];
		context.write(
				key,
				new ArrayWritable(IntWritable.class, offsets
						.toArray(resultArray)));
	}
}
