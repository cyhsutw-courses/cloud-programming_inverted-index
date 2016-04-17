package query;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.alibaba.fastjson.JSON;

public class IndexMapper extends
		Mapper<LongWritable, Text, DocumentSimilarityPair, ScoreArrayWritable> {

	private final class IndexSet {
		public String term;
		public long documentFreq;
		public Map<String, List<Long>> termFreqs = new HashMap<>();

		public IndexSet(String input) {
			String[] items = input.split(";");
			String[] termDf = items[0].split(" ");
			this.term = termDf[0];
			this.documentFreq = Long.parseLong(termDf[1]);

			for (int i = 1; i < items.length; i++) {
				String[] vals = items[i].split(" ");
				termFreqs.put(vals[0], JSON.parseArray(vals[2], Long.class));
			}
		}
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		long numDocs = context.getConfiguration().getLong("document.count", 0);
		List<String> query = Arrays.asList(context.getConfiguration()
				.getStrings("query"));

		for (String val : value.toString().split("\n")) {
			if (val.contains(";")) {
				IndexSet indexSet = new IndexSet(val);
				if (query.contains(indexSet.term)) {
					double invertedDocumentFreq = Math
							.log(((double) indexSet.documentFreq) / numDocs);
					for (Entry<String, List<Long>> entry : indexSet.termFreqs
							.entrySet()) {
						ScoreWritable[] singleElementArr = { new ScoreWritable(
								indexSet.term, invertedDocumentFreq,
								entry.getValue()) };
						System.out
								.println(indexSet.term
										+ " "
										+ entry.getKey()
										+ " "
										+ singleElementArr[0].getOffsets()
												.size());
						context.write(new DocumentSimilarityPair(
								entry.getKey(), 0.0), new ScoreArrayWritable(
								singleElementArr));
					}
				}
			}
		}
	}
}
