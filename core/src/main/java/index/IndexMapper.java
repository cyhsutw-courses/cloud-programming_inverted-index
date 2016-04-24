package index;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

public class IndexMapper extends
		Mapper<LongWritable, Text, IndexKey, TermFrequencyWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		List<String> fileNames = Arrays.asList(context.getConfiguration().getStrings("inputFiles"));
		
		String fileName = ((FileSplit) context.getInputSplit()).getPath()
				.getName();
		
		int fileID = fileNames.indexOf(fileName) + 1;
		
		StandardAnalyzer analyzer = new StandardAnalyzer();

		TokenStream stream = analyzer.tokenStream(null,
				new StringReader(value.toString()));
		stream.reset();

		OffsetAttribute offsetAttribute = stream
				.getAttribute(OffsetAttribute.class);
		CharTermAttribute termAttribute = stream
				.getAttribute(CharTermAttribute.class);

		while (stream.incrementToken()) {
			String toProcess = termAttribute.toString();
			toProcess = toProcess.toLowerCase();
			toProcess = toProcess.replaceAll("[^a-z0-9]", "");

			ArrayList<Long> offsets = new ArrayList<>();
			offsets.add(new Long(key.get() + offsetAttribute.startOffset()));

			context.write(new IndexKey(toProcess, Integer.toString(fileID)),
					new TermFrequencyWritable(Integer.toString(fileID), offsets));
		}

		stream.close();
		analyzer.close();
	}
}
