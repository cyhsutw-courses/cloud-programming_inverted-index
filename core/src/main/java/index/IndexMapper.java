package index;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

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
		String fileName = ((FileSplit) context.getInputSplit()).getPath()
				.getName();

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
			toProcess = toProcess.replaceAll("[^a-zA-Z0-9]", "");
			ArrayList<Integer> offsets = new ArrayList<>();
			offsets.add(new Integer(offsetAttribute.startOffset()));

			context.write(new IndexKey(toProcess, fileName),
					new TermFrequencyWritable(fileName, offsets));
		}

		stream.close();
		analyzer.close();
	}
}
