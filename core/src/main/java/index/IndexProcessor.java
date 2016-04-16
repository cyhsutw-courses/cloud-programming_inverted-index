package index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexProcessor {
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
		config.set("mapreduce.output.textoutputformat.separator", ";");
		config.set("numnum", "44");

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

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
