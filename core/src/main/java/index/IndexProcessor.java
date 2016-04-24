package index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexProcessor {

	private IndexProcessor() {
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
		config.set("mapreduce.output.textoutputformat.separator", ";");

		Path inputPath = new Path(args[0]);
		FileSystem fs = FileSystem.get(config);
		RemoteIterator<LocatedFileStatus> filesIter = fs.listFiles(inputPath, false);
		
		List<String> fileNames = new ArrayList<>();
		while (filesIter.hasNext()) {
			LocatedFileStatus fileStatus = filesIter.next();
			fileNames.add(fileStatus.getPath().getName());
		}

		config.setStrings("inputFiles", fileNames.toArray(new String[fileNames.size()]));
		
		fs.close();
		
		Job job = Job.getInstance(config, "IndexProcessor");
		job.setJarByClass(IndexProcessor.class);

		job.setMapperClass(IndexMapper.class);

		job.setSortComparatorClass(IndexKeyComparator.class);

		job.setCombinerClass(IndexCombiner.class);
		job.setCombinerKeyGroupingComparatorClass(IndexKeyComparator.class);

		job.setGroupingComparatorClass(IndexKeyGroupComparator.class);

		job.setPartitionerClass(IndexKeyPartitioner.class);
		job.setReducerClass(IndexReducer.class);

		job.setNumReduceTasks(4);

		job.setMapOutputKeyClass(IndexKey.class);
		job.setMapOutputValueClass(TermFrequencyWritable.class);

		job.setOutputKeyClass(OutputIndexKey.class);
		job.setOutputValueClass(TermFrequencyArrayWritable.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
