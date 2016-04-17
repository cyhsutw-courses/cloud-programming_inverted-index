package query;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QueryReducer extends
		Reducer<Text, ScoreWritable, Text, ScoreWritable> {

	@Override
	public void reduce(Text key, Iterable<ScoreWritable> values, Context context)
			throws IOException, InterruptedException {

	}
}
