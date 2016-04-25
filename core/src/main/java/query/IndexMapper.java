package query;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Configuration config = context.getConfiguration();
		long numDocs = config.getLong("document.count", 0);
		List<String> query = Arrays.asList(config.getStrings("query"));

		for (String val : value.toString().split("\n")) {
			if (val.contains(";")) {
				IndexSet indexSet = new IndexSet(val);
				if (query.contains(indexSet.term)) {
					double invertedDocumentFreq = Math
							.log10((numDocs / (double) indexSet.documentFreq));
					
					Path pt = new Path("tmp/" + indexSet.term);
                    FileSystem fs = FileSystem.get(config);
                    BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
                    br.write(Double.toString(invertedDocumentFreq)+"\n");
                    br.close();
					
					for (Entry<String, List<Long>> entry : indexSet.termFreqs
							.entrySet()) {
						ScoreWritable[] singleElementArr = { new ScoreWritable(
								indexSet.term, invertedDocumentFreq,
								entry.getValue()) };
						context.write(new DocumentSimilarityPair(
								entry.getKey(), 0.0), new ScoreArrayWritable(
								singleElementArr));
					}
				}
			}
		}
	}
}
