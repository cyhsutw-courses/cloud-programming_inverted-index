package query;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QueryResultReducer extends
		Reducer<DocumentSimilarityPair, ScoreArrayWritable, Text, Text> {
	@Override
	public void reduce(DocumentSimilarityPair key,
			Iterable<ScoreArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(key.getDocName()),
				new Text(Double.toString(key.getSimilarity())));
	}
}
