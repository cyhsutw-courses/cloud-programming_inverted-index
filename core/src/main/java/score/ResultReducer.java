package score;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ResultReducer extends Reducer<DocSimPair, Text, Text, Text>{
	public void reduce(DocSimPair key,
			Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int rank = context.getConfiguration().getInt("rank", 1);
		
		for (Text t : values) {
			context.write(new Text("Rank " + rank + ": "+ key.getDocName() +"\tscore = " + key.getSimilarity()), t);
		}
		
		context.getConfiguration().setInt("rank", rank + 1);
	}
}
