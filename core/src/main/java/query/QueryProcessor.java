package query;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class QueryProcessor {

	private QueryProcessor() {
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();

		// lower case, eliminate special chars, unify white spaces
		String cleanQuery = args[2].trim().toLowerCase()
				.replaceAll("[^a-z0-9]", "").replaceAll("\\s+", " ");

		// for query contains 'or'
		if (cleanQuery.matches(" or ")) {
			config.setBoolean("query.or", true);
			// assuming the each token does not contain whitespace
			config.setStrings("query", cleanQuery.split(" or "));
		} else {
			if (cleanQuery.matches("^\".+\"$")) {
				config.setBoolean("query.exactMatch", true);
			}
			config.setStrings("query", cleanQuery.split(" "));
		}

		FileSystem fileSystem = FileSystem.get(config);
		config.setLong("document.count",
				fileSystem.getContentSummary(new Path(args[0])).getFileCount());
		fileSystem.close();

		Job job = Job.getInstance(config, "QueryProcessor");
		job.setJarByClass(QueryProcessor.class);

		job.setMapperClass(IndexMapper.class);
		job.setSortComparatorClass(DocSimComparator.class);
		job.setCombinerClass(SimilarityCombiner.class);
		job.setCombinerKeyGroupingComparatorClass(DocSimGroupComparator.class);
		job.setReducerClass(QueryReducer.class);

		job.setMapOutputKeyClass(DocumentSimilarityPair.class);
		job.setMapOutputValueClass(ScoreArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
