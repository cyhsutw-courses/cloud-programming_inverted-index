package tf_idf;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TfIdfMapper extends
		Mapper<LongWritable, Text, TfIdfKey, IntWritable> {

	private IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String fileName = ((FileSplit) context.getInputSplit()).getPath()
				.getName();

		StringTokenizer tokenizer = new StringTokenizer(value.toString());

		while (tokenizer.hasMoreTokens()) {
			String toProcess = tokenizer.nextToken();

			toProcess = toProcess.replaceAll("[^a-zA-Z0-9]", "");

			if (toProcess.length() > 0) {
				context.write(new TfIdfKey(toProcess, fileName), one);
			}

		}
	}
}
