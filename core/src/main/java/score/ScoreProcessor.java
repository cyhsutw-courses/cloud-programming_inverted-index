package score;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ScoreProcessor {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
		config.set("textinputformat.record.delimiter", "\n");
		config.set("mapreduce.output.textoutputformat.separator", "\n");
		
		// lower case, eliminate special chars, unify white spaces
		String cleanQuery = args[2].trim().toLowerCase()
				.replaceAll("\\s+", " ").replaceAll("[^a-z0-9 ]", "");

		// for query contains 'or'
		if (cleanQuery.matches(".*( or ).*")) {
			config.setBoolean("query.or", true);
			// assuming the each token does not contain whitespace
			config.setStrings("query", cleanQuery.split(" or "));
		} else {
			if (cleanQuery.matches("^\".+\"$")) {
				config.setBoolean("query.exactMatch", true);
				cleanQuery = cleanQuery.substring(1, cleanQuery.length() - 1);
			}
			String [] qs = cleanQuery.split(" ");
			config.setStrings("query", qs);
			if (qs.length == 1) {
				config.setBoolean("query.or", true);
			}
		}
		config.set("rawPath", args[0]);
		
		Path inputPath = new Path(args[0]);
		FileSystem fs = FileSystem.get(config);
		RemoteIterator<LocatedFileStatus> filesIter = fs.listFiles(inputPath, false);
		
		List<String> fileNames = new ArrayList<>();
		while (filesIter.hasNext()) {
			LocatedFileStatus fileStatus = filesIter.next();
			fileNames.add(fileStatus.getPath().getName());
		}

		config.setStrings("inputFiles", fileNames.toArray(new String[fileNames.size()]));
		
		Job job = Job.getInstance(config, "ScoreProcessor");
		job.setJarByClass(ScoreProcessor.class);

		job.setMapperClass(IndexMapper.class);
		job.setSortComparatorClass(DocSimSortComparator.class);
		job.setReducerClass(ResultReducer.class);

		job.setMapOutputKeyClass(DocSimPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
